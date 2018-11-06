from __future__ import unicode_literals

import glob
import os
import shutil
import socket
import sys
import tempfile
from random import randint

import django

from prometheus_client import core
from prometheus_client.core import (
    CollectorRegistry, Counter, Gauge, Histogram, Sample, Summary,
)
from prometheus_client.multiprocess import (
    MultiProcessCollector,
)

if sys.version_info < (2, 7):
    # We need the skip decorators from unittest2 on Python 2.6.
    import unittest2 as unittest
else:
    import unittest

from unittest.mock import Mock, patch

hostname = socket.gethostname()


class TestDistributed(unittest.TestCase):
    def patch(self, *args, **kwargs) -> Mock:
        p = patch(*args, **kwargs)
        mock = p.start()
        self.addCleanup(p.stop)
        return mock

    def setUp(self):
        from django.conf import settings
        if not settings.configured:
            settings.configure(
                CACHES={
                    'default': {
                        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
                        'LOCATION': 'extra-fast-sqlite-test',
                    }
                }
            )

        self.pid = None
        from prometheus_client.distributed import _pidFunc
        self._original_pidFunc = _pidFunc
        self.patch('prometheus_client.distributed._pidFunc', new=self.pidFunc)

        from prometheus_client.distributed import DistributedValue
        core._ValueClass = DistributedValue
        self.registry = CollectorRegistry()
        from prometheus_client.distributed import DistributedCollector
        self.collector = DistributedCollector(self.registry)

    def pidFunc(self):
        if self.pid is not None:
            return self.pid
        return self._original_pidFunc()

    def tearDown(self):
        core._ValueClass = core._MutexValue

    def test_counter_adds(self):
        c1 = Counter('c2', 'help', registry=None)
        c2 = Counter('c2', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('c2_total'))
        c1.inc(1)
        c2.inc(2)
        self.assertEqual(3, self.registry.get_sample_value('c2_total'))

    def test_summary_adds(self):
        s1 = Summary('s', 'help', registry=None)
        s2 = Summary('s', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('s_count'))
        self.assertEqual(0, self.registry.get_sample_value('s_sum'))
        s1.observe(1)
        s2.observe(2)
        self.assertEqual(2, self.registry.get_sample_value('s_count'))
        self.assertEqual(3, self.registry.get_sample_value('s_sum'))

    def test_histogram_adds(self):
        h1 = Histogram('h', 'help', registry=None)
        h2 = Histogram('h', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('h_count'))
        self.assertEqual(0, self.registry.get_sample_value('h_sum'))
        self.assertEqual(0, self.registry.get_sample_value('h_bucket', {'le': '5.0'}))
        h1.observe(1)
        h2.observe(2)
        self.assertEqual(2, self.registry.get_sample_value('h_count'))
        self.assertEqual(3, self.registry.get_sample_value('h_sum'))
        self.assertEqual(2, self.registry.get_sample_value('h_bucket', {'le': '5.0'}))

    # def test_gauge_all(self):
    #     self.pid = 123
    #     with self.assertRaises(Exception) as e:
    #         g1 = Gauge('g1', 'help', registry=None)
    #     self.assertTrue('not supported' in str(e.exception))

    def test_gauge_liveall(self):
        self.pid = 123
        g1 = Gauge('g2', 'help', registry=None, multiprocess_mode='liveall')
        self.pid = 456
        g2 = Gauge('g2', 'help', registry=None, multiprocess_mode='liveall')
        self.assertEqual(0, self.registry.get_sample_value('g2', {'hostname':hostname,'pid': '123'}))
        self.assertEqual(0, self.registry.get_sample_value('g2', {'hostname':hostname,'pid': '456'}))
        self.pid = 123
        g1.set(1)
        self.pid = 456
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('g2', {'hostname':hostname,'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g2', {'hostname':hostname,'pid': '456'}))
        from prometheus_client.distributed import mark_distributed_process_dead
        mark_distributed_process_dead(123)
        self.assertEqual(None, self.registry.get_sample_value('g2', {'hostname':hostname,'pid': '123'}))
        self.assertEqual(2, self.registry.get_sample_value('g2', {'hostname':hostname,'pid': '456'}))

    def test_gauge_min(self):
        self.pid = 123
        g1 = Gauge('gm', 'help', registry=None, multiprocess_mode='min')
        self.pid = 456
        g2 = Gauge('gm', 'help', registry=None, multiprocess_mode='min')
        self.assertEqual(0, self.registry.get_sample_value('gm', {'hostname':hostname}))
        self.pid = 123
        g1.set(1)
        self.pid = 456
        g2.set(2)
        self.assertEqual(1, self.registry.get_sample_value('gm', {'hostname':hostname}))

    def test_gauge_max(self):
        self.pid = 123
        g1 = Gauge('gmax', 'help', registry=None, multiprocess_mode='max')
        self.pid = 456
        g2 = Gauge('gmax', 'help', registry=None, multiprocess_mode='max')
        self.assertEqual(0, self.registry.get_sample_value('gmax', {'hostname':hostname}))
        self.pid = 123
        g1.set(1)
        self.pid = 456
        g2.set(2)
        self.assertEqual(2, self.registry.get_sample_value('gmax', {'hostname':hostname}))

    def test_gauge_livesum(self):
        self.pid = 123
        g1 = Gauge('gls', 'help', registry=None, multiprocess_mode='livesum')
        self.pid = 456
        g2 = Gauge('gls', 'help', registry=None, multiprocess_mode='livesum')
        self.assertEqual(0, self.registry.get_sample_value('gls', {'hostname':hostname}))
        self.pid = 123
        g1.set(1)
        self.pid = 456
        g2.set(2)
        self.assertEqual(3, self.registry.get_sample_value('gls', {'hostname':hostname}))
        from prometheus_client.distributed import mark_distributed_process_dead
        mark_distributed_process_dead(123)
        self.assertEqual(2, self.registry.get_sample_value('gls', {'hostname':hostname}))

    def test_namespace_subsystem(self):
        c1 = Counter('c', 'help', registry=None, namespace='ns', subsystem='ss')
        c1.inc(1)
        self.assertEqual(1, self.registry.get_sample_value('ns_ss_c_total'))

    def test_counter_across_forks(self):
        self.pid = 0
        c1 = Counter('c', 'help', registry=None)
        self.assertEqual(0, self.registry.get_sample_value('c_total'))
        c1.inc(1)
        c1.inc(1)
        self.pid = 1
        c1.inc(1)
        self.assertEqual(3, self.registry.get_sample_value('c_total'))
        self.assertEqual(1, c1._value.get())

    @unittest.skipIf(sys.version_info < (2, 7), "Test requires Python 2.7+.")
    def test_collect(self):
        self.pid = 0

        labels = dict((i, i) for i in 'abcd')

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        c = Counter('c', 'help', labelnames=labels.keys(), registry=None)
        g = Gauge('g', 'help', labelnames=labels.keys(), registry=None, multiprocess_mode='liveall')
        gall = Gauge('gall', 'help', labelnames=labels.keys(), registry=None, multiprocess_mode='all')
        gempty = Gauge('gempty', 'help', labelnames=labels.keys(), registry=None, multiprocess_mode='all')
        h = Histogram('h', 'help', labelnames=labels.keys(), registry=None)

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        gall.labels(**labels).set(1)
        h.labels(**labels).observe(1)

        self.pid = 1

        c.labels(**labels).inc(1)
        g.labels(**labels).set(1)
        gall.labels(**labels).set(1)
        h.labels(**labels).observe(5)

        metrics = dict((m.name, m) for m in self.collector.collect())

        self.assertEqual(
            metrics['c'].samples, [Sample('c_total', labels, 2.0)]
        )
        metrics['g'].samples.sort(key=lambda x: x[1]['pid'])
        for sample in metrics['g'].samples:
            sample.labels.pop('hostname')
        for sample in metrics['gall'].samples:
            sample.labels.pop('hostname')
        self.assertEqual(metrics['g'].samples, [
            Sample('g', add_label('pid', '0'), 1.0),
            Sample('g', add_label('pid', '1'), 1.0),
        ])
        self.assertEqual(metrics['gall'].samples, [
            Sample('gall', add_label('pid', '0'), 1.0),
            Sample('gall', add_label('pid', '1'), 1.0),
        ])

        metrics['h'].samples.sort(
            key=lambda x: (x[0], float(x[1].get('le', 0)))
        )
        expected_histogram = [
            Sample('h_bucket', add_label('le', '0.005'), 0.0),
            Sample('h_bucket', add_label('le', '0.01'), 0.0),
            Sample('h_bucket', add_label('le', '0.025'), 0.0),
            Sample('h_bucket', add_label('le', '0.05'), 0.0),
            Sample('h_bucket', add_label('le', '0.075'), 0.0),
            Sample('h_bucket', add_label('le', '0.1'), 0.0),
            Sample('h_bucket', add_label('le', '0.25'), 0.0),
            Sample('h_bucket', add_label('le', '0.5'), 0.0),
            Sample('h_bucket', add_label('le', '0.75'), 0.0),
            Sample('h_bucket', add_label('le', '1.0'), 1.0),
            Sample('h_bucket', add_label('le', '2.5'), 1.0),
            Sample('h_bucket', add_label('le', '5.0'), 2.0),
            Sample('h_bucket', add_label('le', '7.5'), 2.0),
            Sample('h_bucket', add_label('le', '10.0'), 2.0),
            Sample('h_bucket', add_label('le', '+Inf'), 2.0),
            Sample('h_count', labels, 2.0),
            Sample('h_sum', labels, 6.0),
        ]

        self.assertEqual(metrics['h'].samples, expected_histogram)

    @unittest.skipIf(sys.version_info < (2, 7), "Test requires Python 2.7+.")
    def test_merge_no_accumulate(self):
        self.pid = 0
        labels = dict((i, i) for i in 'abcd')

        def add_label(key, value):
            l = labels.copy()
            l[key] = value
            return l

        h = Histogram('hna', 'help', labelnames=labels.keys(), registry=None)
        h.labels(**labels).observe(1)
        self.pid = 1
        h.labels(**labels).observe(5)

        self.collector.accumulate = False
        metrics = self.collector.collect()
        self.collector.accumulate = True

        metric = [x for x in metrics if x.name == 'hna'][0]
        metric.samples.sort(
            key=lambda x: (x[0], float(x[1].get('le', 0)))
        )
        expected_histogram = [
            Sample('hna_bucket', add_label('le', '0.005'), 0.0),
            Sample('hna_bucket', add_label('le', '0.01'), 0.0),
            Sample('hna_bucket', add_label('le', '0.025'), 0.0),
            Sample('hna_bucket', add_label('le', '0.05'), 0.0),
            Sample('hna_bucket', add_label('le', '0.075'), 0.0),
            Sample('hna_bucket', add_label('le', '0.1'), 0.0),
            Sample('hna_bucket', add_label('le', '0.25'), 0.0),
            Sample('hna_bucket', add_label('le', '0.5'), 0.0),
            Sample('hna_bucket', add_label('le', '0.75'), 0.0),
            Sample('hna_bucket', add_label('le', '1.0'), 1.0),
            Sample('hna_bucket', add_label('le', '2.5'), 0.0),
            Sample('hna_bucket', add_label('le', '5.0'), 1.0),
            Sample('hna_bucket', add_label('le', '7.5'), 0.0),
            Sample('hna_bucket', add_label('le', '10.0'), 0.0),
            Sample('hna_bucket', add_label('le', '+Inf'), 0.0),
            Sample('hna_sum', labels, 6.0),
        ]

        self.assertEqual(metric.samples, expected_histogram)


class TestUnsetEnv(unittest.TestCase):
    def setUp(self):
        self.registry = CollectorRegistry()
        fp, self.tmpfl = tempfile.mkstemp()
        os.close(fp)

    def test_unset_syncdir_env(self):
        self.assertRaises(
            ValueError, MultiProcessCollector, self.registry)

    def test_file_syncpath(self):
        registry = CollectorRegistry()
        self.assertRaises(
            ValueError, MultiProcessCollector, registry, self.tmpfl)

    def tearDown(self):
        os.remove(self.tmpfl)
