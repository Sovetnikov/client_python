# Prometheus Python Client

This prometheus client has ability to store all metrics in django cache.
Just set environment variable "prometheus_django_cache" to django cache name, like "default".

## Internals
Each process has separate metrics storage in one cache entry.
Each process is identified by hostname+pid.

To allow collector to find all instances that stored any metrics there is global list of all available hostname+pid identifiers stored in cache. Read+Write access to that list is synchronized by distributed lock based on cache.
Process added to that list just once, when first metric value is stored.

Gauge modes work like with "prometheus_multiproc_dir", but mode "all" is working like "liveall".

distributed.mark_process_dead works like multiprocess.mark_process_dead but removes all gauge modes include mode "all". 

Metric storing process has no locking mechanism for read and write because metrics storage is per process with assumption that there is no theading. 

## Advantages
* any distributed application will work out of the box
* no FD leaking like in multiprocessing mode with uwsgi
* no files stored on disk
* theoretically any django cache backend will work (but I have some troubles with memcached on practice)
* because of django cache api simplicity, any storage can be simple plugged in 

## Disadvantages and things to do
* metrics stored in cache have ttl (5 days by now) and old counters can be evicted so counter value will decrease (ttl refreshing is planned, but django 1.7 cache API is not supported touch method).
* maybe performance can suffer because of network overhead (no performance tests are done by now)
* hostname+pid list has no cleaning process and can grow up

## Requirements
* Django 1.7+ supported