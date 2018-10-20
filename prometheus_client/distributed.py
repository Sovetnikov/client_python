#!/usr/bin/python

from __future__ import unicode_literals

import inspect
import json
import os
import socket
import time
from collections import defaultdict
from threading import Lock

from django.core.cache import caches
cache = caches[os.environ.get('prometheus_django_cache','default')]

hostname = socket.gethostname()

in_lock = None

lock = Lock()

class CacheLock(object):
    def __init__(self, lock_id, ttl):
        self.id = 'cachelock-{0}'.format(lock_id)
        self.ttl = ttl
        self.status = False
        self.timeout_at = None

    def __nonzero__(self):
        return self.status

    def __enter__(self):
        global in_lock
        if in_lock:
            raise Exception('Already on lock ' + in_lock)
        trys = 6
        while trys:
            self.timeout_at = time.monotonic() + self.ttl
            self.status = cache.add(self.id, 'locked', self.ttl)
            if self.status:
                in_lock = self.id
                return self.status
            time.sleep(0.1)
            trys -= 1
        raise Exception('Could not lock for {self.id}'.format(**locals()))

    def __exit__(self, type, value, tb):
        global in_lock
        in_lock = None
        if self.status:
            if time.monotonic() < self.timeout_at:
                cache.delete(self.id)
            if self.id in cache:
                raise Exception('Id in cache ' + self.id)


distributed_list_cache_key = 'pc_distributed_list'
distributed_list_lock = CacheLock(distributed_list_cache_key, ttl=20)
added_to_distributed_list = set()

distributed_list_ttl_minutes = 60*24*5
distributed_value_ttl_minutes = 60*24*5

def add_to_distributed_list(pid):
    if not (hostname, pid) in added_to_distributed_list:
        with distributed_list_lock:
            l = cache.get(distributed_list_cache_key, set())
            l.add((hostname, pid))
            cache.set(distributed_list_cache_key, l, distributed_list_ttl_minutes*60)
            added_to_distributed_list.add((hostname, pid))


def remove_from_distributed_list(pid):
    with distributed_list_lock:
        l = cache.get(distributed_list_cache_key, set())

        def _iterate():
            if isinstance(pid, int):
                yield pid
            else:
                for p in pid:
                    yield p

        if (hostname, pid) in l:
            l.remove((hostname, pid))
        if (hostname, pid) in added_to_distributed_list:
            added_to_distributed_list.remove((hostname, pid))

        cache.set(distributed_list_cache_key, l, distributed_list_ttl_minutes*60)


_pidFunc = os.getpid

__cached_get_all_typ = None


def get_all_typ():
    from . import core

    global __cached_get_all_typ
    if __cached_get_all_typ is None:
        __cached_get_all_typ = []
        for name, obj in inspect.getmembers(core):
            if inspect.isclass(obj):
                typ = getattr(obj, '_type', None)
                if typ:
                    __cached_get_all_typ.append(typ)
            else:
                wrapper = getattr(obj, '__wrapped__', None)
                if wrapper:
                    typ = getattr(wrapper, '_type', None)
                    if typ:
                        __cached_get_all_typ.append(typ)
        __cached_get_all_typ.extend([core.Gauge.__wrapped__._type + '_' + mode for mode in core.Gauge.__wrapped__._MULTIPROC_MODES])
    return __cached_get_all_typ


prometheus_cache_key_prefix = 'pcachekey.'


def get_cache_key(typ, hostname, pid, prometheus_cache_key_prefix=prometheus_cache_key_prefix):
    return '{prometheus_cache_key_prefix}{typ};{hostname};{pid}'.format(**locals())


def deconstruct_cache_key(cache_key):
    l = cache_key.replace(prometheus_cache_key_prefix, '').split(';')
    return l


class DistributedValue(object):
    _multiprocess = False

    def __init__(self, typ, metric_name, name, labelnames, labelvalues, multiprocess_mode='', **kwargs):
        if typ == 'gauge':
            # if multiprocess_mode == 'all':
            #     raise Exception('multiprocess_mode=all not supported in distributed storage')
            typ_prefix = typ + '_' + multiprocess_mode
        else:
            typ_prefix = typ
        self.typ_prefix = typ_prefix
        from . import core

        self.valuekey = core._mmap_key(metric_name, name, labelnames, labelvalues)
        self.__reset()

    @property
    def cachekey(self):
        pid = _pidFunc()
        add_to_distributed_list(pid)
        return get_cache_key(self.typ_prefix, hostname, pid)

    def __get_dict(self):
        return cache.get(self.cachekey, {})

    def __set_dict(self, dict_value):
        cache.set(self.cachekey, dict_value, distributed_value_ttl_minutes)

    def __reset(self):
        # with lock:
        d = self.__get_dict()
        if not self.valuekey in d:
            d[self.valuekey] = 0
            self.__set_dict(d)

    def inc(self, amount):
        # with lock:
        d = self.__get_dict()
        d[self.valuekey] = d.get(self.valuekey, 0) + amount
        self.__set_dict(d)

    def set(self, value):
        # with lock:
        d = self.__get_dict()
        d[self.valuekey] = value
        self.__set_dict(d)

    def get(self):
        # with lock:
        return self.__get_dict().get(self.valuekey, None)


class DistributedCollector(object):
    def __init__(self, registry):
        if registry:
            registry.register(self)
        self.accumulate = True

    def collect(self):
        cache_keys = set()
        distributed_list = cache.get(distributed_list_cache_key, set())
        for hostname, pid in distributed_list:
            for typ in get_all_typ():
                cache_keys.add(get_cache_key(typ, hostname, pid))

        return self.merge(cache.get_many(cache_keys), accumulate=self.accumulate)

    def merge(self, cache_values, accumulate=True):
        """Merge metrics from given mmap files.

        By default, histograms are accumulated, as per prometheus wire format.
        But if writing the merged data back to mmap files, use
        accumulate=False to avoid compound accumulation.
        """
        from . import core
        metrics = {}
        for cache_key, cache_value in cache_values.items():
            typ, value_hostname, pid = deconstruct_cache_key(cache_key)
            multiprocess_mode = None
            if '_' in typ:
                typ, multiprocess_mode = typ.split('_')
            for key, value in cache_value.items():
                metric_name, name, labels = json.loads(key)
                labels_key = tuple(sorted(labels.items()))

                metric = metrics.get(metric_name)
                if metric is None:
                    metric = core.Metric(metric_name, 'Multiprocess metric', typ)
                    metrics[metric_name] = metric

                if typ == 'gauge':
                    metric._multiprocess_mode = multiprocess_mode
                    metric.add_sample(name, labels_key + (('pid', pid),
                                                          ('hostname', value_hostname)
                                                          ), value)
                else:
                    # The duplicates and labels are fixed in the next for.
                    metric.add_sample(name, labels_key, value)

        for metric in metrics.values():
            samples = defaultdict(float)
            buckets = {}
            for s in metric.samples:
                name, labels, value = s.name, s.labels, s.value
                if metric.type == 'gauge':
                    without_pid = tuple(l for l in labels if l[0] != 'pid')
                    if metric._multiprocess_mode == 'min':
                        current = samples.setdefault((name, without_pid), value)
                        if value < current:
                            samples[(s.name, without_pid)] = value
                    elif metric._multiprocess_mode == 'max':
                        current = samples.setdefault((name, without_pid), value)
                        if value > current:
                            samples[(s.name, without_pid)] = value
                    elif metric._multiprocess_mode == 'livesum':
                        samples[(name, without_pid)] += value
                    else:  # all/liveall
                        samples[(name, labels)] = value

                elif metric.type == 'histogram':
                    bucket = tuple(float(l[1]) for l in labels if l[0] == 'le')
                    if bucket:
                        # _bucket
                        without_le = tuple(l for l in labels if l[0] != 'le')
                        buckets.setdefault(without_le, {})
                        buckets[without_le].setdefault(bucket[0], 0.0)
                        buckets[without_le][bucket[0]] += value
                    else:
                        # _sum/_count
                        samples[(s.name, labels)] += value

                else:
                    # Counter and Summary.
                    samples[(s.name, labels)] += value

            # Accumulate bucket values.
            if metric.type == 'histogram':
                for labels, values in buckets.items():
                    acc = 0.0
                    for bucket, value in sorted(values.items()):
                        sample_key = (
                            metric.name + '_bucket',
                            labels + (('le', core._floatToGoString(bucket)),),
                        )
                        if accumulate:
                            acc += value
                            samples[sample_key] = acc
                        else:
                            samples[sample_key] = value
                    if accumulate:
                        samples[(metric.name + '_count', labels)] = acc

            # Convert to correct sample format.
            metric.samples = [core.Sample(name, dict(labels), value) for (name, labels), value in samples.items()]
        return metrics.values()


def mark_distributed_process_dead(pid):
    remove_from_distributed_list(pid)
