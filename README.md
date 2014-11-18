SharQ
=======

SharQ is a flexible rate limited queueing system built using [Redis](http://redis.io). SharQ is the core library that powers the [SharQ Server](https://github.com/plivo/sharq-server).

## Installation

* Install the [latest stable release of Redis](http://redis.io/download).
* Install SharQ using pip
```
pip install sharq
```

## Configuration

Sample SharQ Config file.
```
[sharq]
job_expire_interval       : 1000 ; in milliseconds
job_requeue_interval      : 1000 ; in milliseconds
default_job_requeue_limit : -1 ; retries infinitely

[redis]
db                        : 0
key_prefix                : sharq_server
conn_type                 : tcp_sock ; or unix_sock
;; unix connection settings
unix_socket_path          : /tmp/redis.sock
;; tcp connection settings
port                      : 6379
host                      : 127.0.0.1
```

__Note:__ Uncomment the following lines in your `redis.conf` if you are using unix socket to connect to Redis.
```
unixsocket /var/run/redis/redis.sock
unixsocketperm 755
```

## Usage

### Initialization

```python
>>> from sharq import SharQ

>>> sq = SharQ('/path/to/config/sharq.conf')
```

### Enqueue

Enqueues a job into the queue. Every enqueue request is accompanied with an `interval`. The interval specifies the rate limiting capability of SharQ. An interval of 1000ms implies that SharQ will ensure two successful dequeue requests will be separated by 1000ms (interval is the inverse of rate. 1000ms interval means 1 job per second)

```python
>>> response = sq.enqueue(
	    job_id='cea84623-be35-4368-90fa-7736570dabc4',
		payload={'message': 'hello, world'},
		interval=1000,  # in milliseconds.
		queue_id='user001',
		queue_type='sms'  # optional. defaults to 'default' queue type.
	)
>>> print response
{'status': 'queued'}
```
### Dequeue

Dequeues a job (non-blocking). It returns a job only if available or if it is ready for dequeue (based on the interval set while enqueueing).

```python
>>> response = sq.dequeue(
	    queue_type='sms'  # optional.
	)
>>> print response  # when the queue is empty or no job is ready.
{'status': 'failure'}
>>> print response  # when the job is ready.
{'job_id': 'cea84623-be35-4368-90fa-7736570dabc4',
 'payload': {'message': 'hello, world'},
 'queue_id': 'johndoe',
 'status': 'success'}
```

### Finish

Marks any dequeued job as _succesfully completed_. Any job which does get marked as finished upon dequeue will be re-enqueued into its respective queue after an expiry time (the `job_requeue_interval` in the config).

```python
>>> response = sq.finish(
	    queue_type='sms',
		job_id='bb59a2be-3b48-4645-8134-d9181742e3cf',
		queue_id='user001'
	)
>>> print response
{'status': 'success'}
```

### Requeue

Ee-queues all the jobs which do not get the finish (ACK) within the expiry time (the `job_requeue_interval` in the config file).

```python
>>> response = sq.requeue()  # re-queues all expired jobs.
>>> print response
None
```

### Interval

Updates the interval for a specified queue on the fly. The interval specifies the rate limiting capability of SharQ. An interval of 1000ms implies that SharQ will ensure two successful dequeue requests will be separated by 1000ms (interval is the inverse of rate. 1000ms interval means 1 job per second).

```python
>>> response = sq.interval(
	    queue_type='sms',
		interval=5000,  # interval between two successful dequeues is set to 5s
		queue_id='user001'
	)
>>> print response
{'status': 'success'}
```

### Merics

Gets the SharQ metrics like,

* Overall enqueue / dequeue rate.
* Queue specific enqueue / dequeue rate.
* Queue types and queue ids in SharQ.
* Queue length of a particual queue.

```python
>>> response = sq.metrics()  # gets the overall statistics.
>>> print response
{'dequeue_counts': {
   '1406280420000': 10, # epoch timestamp of the minute & the dequeue count.
   '1406280480000': 0,
   '1406280540000': 304,
   '1406280600000': 0,
   '1406280660000': 605,
   '1406280720000': 604,
   '1406280780000': 615,
   '1406280840000': 233,
   '1406280900000': 322,
   '1406280960000': 272},
 'enqueue_counts': {
   '1406280420000': 0,
   '1406280480000': 0,
   '1406280540000': 0,
   '1406280600000': 0,
   '1406280660000': 0,
   '1406280720000': 0,
   '1406280780000': 40,
   '1406280840000': 40,
   '1406280900000': 40,
   '1406280960000': 39},
   'queue_types': ['sms'],
   'status': u'success'}

>>> response = sq.metrics(queue_type='sms')  # gets the queue ids of this type.
>>> print response
{'queue_ids': ['user001', 'user002'], 'status': 'success'}

>>> response = sq.metrics(  # gets the stats for this particular queue.
        queue_type='sms',
        queue_id='user001'
    )
>>> print response
{'dequeue_counts': {
   '1406280420000': 10, # epoch timestamp of the minute & the dequeue count.
   '1406280480000': 0,
   '1406280540000': 304,
   '1406280600000': 0,
   '1406280660000': 605,
   '1406280720000': 604,
   '1406280780000': 615,
   '1406280840000': 233,
   '1406280900000': 322,
   '1406280960000': 272},
 'enqueue_counts': {
   '1406280420000': 0,
   '1406280480000': 0,
   '1406280540000': 0,
   '1406280600000': 0,
   '1406280660000': 0,
   '1406280720000': 0,
   '1406280780000': 40,
   '1406280840000': 40,
   '1406280900000': 40,
   '1406280960000': 39},
   'queue_length': 2400,  # the number of jobs in this queue.
   'status': u'success'}
```

## Development

### Getting the source code

```
git clone https://github.com/plivo/sharq.git
```

### Running Tests

```
make test
```

### Building a Package

```
make build
```

### Install / Uninstall

```
make install
make uninstall
```

## License

```
The MIT License (MIT)

Copyright (c) 2014 Plivo Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
