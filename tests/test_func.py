# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
import os
import uuid
import time
import math
import unittest
import msgpack
from sharq import SharQ
from sharq.utils import generate_epoch


class SharQTestCase(unittest.TestCase):
    """
    `SharQTestCase` contains the functional test cases
    that validate the correctness of all the APIs exposed
    by SharQ.
    """

    def setUp(self):
        cwd = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(cwd, 'sharq.test.conf')  # test config
        self.queue = SharQ(config_path)
        # flush all the keys in the test db before starting test
        self.queue._r.flushdb()
        # test specific values
        self._test_queue_id = 'johndoe'
        self._test_queue_type = 'sms'
        self._test_payload_1 = {
            'to': '1000000000',
            'message': 'Hello, world'
        }
        self._test_payload_2 = {
            'to': '1000000001',
            'message': 'Hello, SharQ'
        }
        self._test_requeue_limit_5 = 5
        self._test_requeue_limit_neg_1 = -1
        self._test_requeue_limit_0 = 0

    def _get_job_id(self):
        """Generates a uuid4 and returns the string
        representation of it.
        """
        return str(uuid.uuid4())

    def test_enqueue_response_status(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        self.assertEqual(response['status'], 'queued')

    def test_enqueue_job_queue_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check if the job queue exists
        queue_name = '%s:%s:%s' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id
        )
        self.assertTrue(self.queue._r.exists(queue_name))

    def test_enqueue_job_existence_in_job_queue(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        # check if the queue contains the job we just pushed (by peeking)
        queue_name = '%s:%s:%s' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id
        )
        latest_job_id = self.queue._r.lrange(queue_name, -1, -1)
        self.assertEqual(latest_job_id, [job_id])

    def test_enqueue_job_queue_length(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check if the queue length is one
        queue_name = '%s:%s:%s' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id
        )
        queue_length = self.queue._r.llen(queue_name)
        self.assertEqual(queue_length, 1)

    def test_enqueue_payload_dump(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check if the payload is saved in the appropriate structure
        payload_map_name = '%s:payload' % (self.queue._key_prefix)
        # check if the payload map exists
        self.assertTrue(self.queue._r.exists(payload_map_name))

    def test_enqueue_payload_encode_decode(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        payload_map_name = '%s:payload' % (self.queue._key_prefix)
        payload_map_key = '%s:%s:%s' % (
            self._test_queue_type, self._test_queue_id, job_id)
        raw_payload = self.queue._r.hget(payload_map_name, payload_map_key)
        # decode the payload from msgpack to dictionary
        payload = msgpack.unpackb(raw_payload[1:-1])
        self.assertEqual(payload, self._test_payload_1)

    def test_enqueue_interval_map_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check if interval is saved in the appropriate structure
        interval_map_name = '%s:interval' % (self.queue._key_prefix)
        # check if interval map exists
        self.assertTrue(self.queue._r.exists(interval_map_name))

    def test_enqueue_interval_value(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check if interval is saved in the appropriate structure
        interval_map_name = '%s:interval' % (self.queue._key_prefix)
        interval_map_key = '%s:%s' % (
            self._test_queue_type, self._test_queue_id)
        interval = self.queue._r.hget(
            interval_map_name, interval_map_key)
        self.assertEqual(interval, '10000')  # 10s (10000ms)

    def test_enqueue_requeue_limit_map_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type
            # without a requeue limit parameter
        )

        # check if requeue limit is saved in the appropriate structure
        requeue_limit_map_name = '%s:%s:%s:requeues_remaining' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        # check if requeue limit  map exists
        self.assertTrue(self.queue._r.exists(requeue_limit_map_name))

        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5
        )

        # check if requeue limit is saved in the appropriate structure
        requeue_limit_map_name = '%s:%s:%s:requeues_remaining' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        # check if requeue limit  map exists
        self.assertTrue(self.queue._r.exists(requeue_limit_map_name))

    def test_enqueue_requeue_limit_value(self):
        # without requeue limit (but reading from the config)
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type
            # without requeue limit.
        )

        # check if requeue limit is saved in the appropriate structure
        requeue_limit_map_name = '%s:%s:%s:requeues_remaining' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        requeues_remaining = self.queue._r.hget(
            requeue_limit_map_name, job_id)
        self.assertEqual(requeues_remaining, '-1')  # from the config file.

        # with requeue limit in the enqueue function.
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5
        )

        # check if requeue limit is saved in the appropriate structure
        requeue_limit_map_name = '%s:%s:%s:requeues_remaining' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
        )
        requeues_remaining = self.queue._r.hget(
            requeue_limit_map_name, job_id)
        self.assertEqual(requeues_remaining, '5') # 5 retries remaining.

    def test_enqueue_ready_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        sorted_set_name = '%s:%s' % (
            self.queue._key_prefix, self._test_queue_type)
        self.assertTrue(self.queue._r.exists(sorted_set_name))

    def test_enqueue_ready_set_contents(self):
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        end_time = str(generate_epoch())

        sorted_set_name = '%s:%s' % (
            self.queue._key_prefix, self._test_queue_type)
        queue_id_list = self.queue._r.zrangebyscore(
            sorted_set_name,
            start_time,
            end_time)
        # check if exactly one item in the list
        self.assertEqual(len(queue_id_list), 1)
        # check the value to match the queue_id
        self.assertEqual(queue_id_list[0], self._test_queue_id)

    def test_enqueue_queue_type_ready_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check the queue type ready set.
        queue_type_ready_set = self.queue._r.smembers(
            '%s:ready:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 1)
        self.assertEqual(queue_type_ready_set.pop(), self._test_queue_type)

    def test_enqueue_queue_type_active_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_type_ready_set = self.queue._r.smembers(
            '%s:active:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 0)

    def test_enqueue_metrics_global_enqueue_counter(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = self.queue._r.get('%s:enqueue_counter:%s' % (
            self.queue._key_prefix, timestamp_minute))
        self.assertEqual(counter_value, '1')

    def test_enqueue_metrics_per_queue_enqueue_counter(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = self.queue._r.get('%s:%s:%s:enqueue_counter:%s' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
            timestamp_minute))
        self.assertEqual(counter_value, '1')

    def test_enqueue_second_job_status(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        self.assertEqual(response['status'], 'queued')

    def test_enqueue_second_job_queue_existence(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_name = '%s:%s:%s' % (
            self.queue._key_prefix, self._test_queue_type, self._test_queue_id)
        self.assertTrue(self.queue._r.exists(queue_name))

    def test_enqueue_second_job_existence_in_job_queue(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_name = '%s:%s:%s' % (
            self.queue._key_prefix, self._test_queue_type, self._test_queue_id)
        latest_job_id = self.queue._r.lrange(queue_name, -1, -1)
        self.assertEqual(latest_job_id, [job_id])

    def test_enqueue_second_job_queue_length(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_name = '%s:%s:%s' % (
            self.queue._key_prefix, self._test_queue_type, self._test_queue_id)
        # check if the queue length is two
        queue_length = self.queue._r.llen(queue_name)
        self.assertEqual(queue_length, 2)

    def test_enqueue_second_job_payload_dump(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        payload_map_name = '%s:payload' % (self.queue._key_prefix)
        # check if the payload map exists
        self.assertTrue(self.queue._r.exists(payload_map_name))

    def test_enqueue_second_job_payload_encode_decode(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        payload_map_name = '%s:payload' % (self.queue._key_prefix)
        payload_map_key = '%s:%s:%s' % (
            self._test_queue_type, self._test_queue_id, job_id)
        raw_payload = self.queue._r.hget(payload_map_name, payload_map_key)
        # decode the payload from msgpack to dictionary
        payload = msgpack.unpackb(raw_payload[1:-1])
        self.assertEqual(payload, self._test_payload_2)

    def test_enqueue_second_job_interval_map_existence(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        interval_map_name = '%s:interval' % (self.queue._key_prefix)
        # check if interval map exists
        self.assertTrue(self.queue._r.exists(interval_map_name))

    def test_enqueue_second_job_interval_value(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        interval_map_name = '%s:interval' % (self.queue._key_prefix)
        interval_map_key = '%s:%s' % (
            self._test_queue_type, self._test_queue_id)
        interval = self.queue._r.hget(interval_map_name, interval_map_key)
        self.assertEqual(interval, '20000')  # 20s (20000ms)

    def test_enqueue_second_job_ready_set(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        sorted_set_name = '%s:%s' % (
            self.queue._key_prefix, self._test_queue_type)
        self.assertTrue(self.queue._r.exists(sorted_set_name))

    def test_enqueue_second_job_ready_set_contents(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # sleeping for 500ms to ensure that the
        # time difference between two enqueues is
        # measurable for the test cases.
        time.sleep(0.5)

        # job 2
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        sorted_set_name = '%s:%s' % (
            self.queue._key_prefix, self._test_queue_type)
        end_time = str(generate_epoch())
        queue_id_list = self.queue._r.zrangebyscore(
            sorted_set_name,
            start_time,
            end_time)
        self.assertEqual(len(queue_id_list), 0)

    def test_enqueue_second_job_queue_type_ready_set(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check the queue type ready set.
        queue_type_ready_set = self.queue._r.smembers(
            '%s:ready:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 1)
        self.assertEqual(queue_type_ready_set.pop(), self._test_queue_type)

    def test_enqueue_second_job_queue_type_active_set(self):
        # job 1
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # job 2
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=20000,  # 20s (20000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        queue_type_ready_set = self.queue._r.smembers(
            '%s:active:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 0)

    def test_dequeue_response_status_failure(self):
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        self.assertEqual(response['status'], 'failure')

    def test_dequeue_response_status_success_without_requeue_limit(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            # without requeue limit
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # check all the responses
        self.assertEqual(response['status'], 'success')
        self.assertEqual(response['queue_id'], self._test_queue_id)
        self.assertEqual(response['job_id'], job_id)
        self.assertEqual(response['payload'], self._test_payload_1)
        self.assertEqual(response['requeues_remaining'], -1)  # from the config

    def test_dequeue_response_status_success_with_requeue_limit(self):
        # with requeue limit passed explicitly
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # check all the responses
        self.assertEqual(response['status'], 'success')
        self.assertEqual(response['queue_id'], self._test_queue_id)
        self.assertEqual(response['job_id'], job_id)
        self.assertEqual(response['payload'], self._test_payload_1)
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_5)

    def test_dequeue_job_queue_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        queue_name = '%s:%s:%s' % (
            self.queue._key_prefix, self._test_queue_type, self._test_queue_id)
        self.assertFalse(self.queue._r.exists(queue_name))

    def test_dequeue_time_keeper_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # time keeper key should exist
        time_keeper_key_name = '%s:%s:%s:time' % (
            self.queue._key_prefix,
            self._test_queue_type, self._test_queue_id
        )
        self.assertTrue(self.queue._r.exists(time_keeper_key_name))

    def test_dequeue_ready_sorted_set_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # the sorted set should not exists
        sorted_set_name = '%s:%s' % (
            self.queue._key_prefix, self._test_queue_type)
        self.assertFalse(self.queue._r.exists(sorted_set_name))

    def test_dequeue_active_sorted_set(self):
        job_id = self._get_job_id()
        start_time = str(generate_epoch())
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # the job should exist in the active set with the timestamp
        # it was picked up with.
        active_sorted_set_name = '%s:%s:active' % (
            self.queue._key_prefix,
            self._test_queue_type
        )
        end_time = str(generate_epoch())

        job_expire_timestamp = str(
            int(end_time) + self.queue._job_expire_interval)
        job_id_list = self.queue._r.zrangebyscore(
            active_sorted_set_name,
            start_time,
            job_expire_timestamp)
        # check if there is exactly one job in the
        # active sorted set
        self.assertEqual(len(job_id_list), 1)

    def test_dequeue_time_keeper_expiry(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=1000,  # 1s (1000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # wait for the interval duration and check that the
        # time keeper should have expired
        time.sleep(self.queue._job_expire_interval / 1000.00)  # in seconds
        time_keeper_key_name = '%s:%s:%s:time' % (
            self.queue._key_prefix,
            self._test_queue_type, self._test_queue_id
        )
        self.assertFalse(self.queue._r.exists(time_keeper_key_name))

    def test_dequeue_ready_queue_type_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # the ready queue type set should have 0 items
        queue_type_ready_set = self.queue._r.smembers(
            '%s:ready:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 0)

    def test_dequeue_active_queue_type_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # the active queue type set should have one item
        queue_type_ready_set = self.queue._r.smembers(
            '%s:active:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 1)
        self.assertEqual(queue_type_ready_set.pop(), self._test_queue_type)

    def test_dequeue_metrics_global_dequeue_counter(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = self.queue._r.get('%s:dequeue_counter:%s' % (
            self.queue._key_prefix, timestamp_minute))
        self.assertEqual(counter_value, '1')

    def test_dequeue_metrics_per_queue_dequeue_counter(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        counter_value = self.queue._r.get('%s:%s:%s:dequeue_counter:%s' % (
            self.queue._key_prefix,
            self._test_queue_type,
            self._test_queue_id,
            timestamp_minute))
        self.assertEqual(counter_value, '1')

    def test_finish_on_empty_queue(self):
        job_id = self._get_job_id()
        response = self.queue.finish(
            job_id=job_id,
            queue_id='doesnotexist',
            queue_type=self._test_queue_type
        )

        self.assertEqual(response['status'], 'failure')

    def test_finish_response_status(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        self.assertEqual(response['status'], 'success')

    def test_finish_ready_sorted_set_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        self.assertFalse(
            self.queue._r.exists('%s:%s' % (
                self.queue._key_prefix, self._test_queue_type)))

    def test_finish_active_sorted_set_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        self.assertFalse(
            self.queue._r.exists('%s:%s:active' % (
                self.queue._key_prefix, self._test_queue_type)))

    def test_finish_payload_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        self.assertFalse(
            self.queue._r.exists('%s:payload' % self.queue._key_prefix))

    def test_finish_interval_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        self.assertFalse(
            self.queue._r.exists('%s:interval' % self.queue._key_prefix))

    def test_finish_requeue_limit_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_0
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # mark the job as finished
        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        self.assertFalse(
            self.queue._r.exists('%s:%s:%s:requeues_remaining' % (
                self.queue._key_prefix, self._test_queue_type, self._test_queue_id
            ))
        )

    def test_finish_job_queue_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        self.assertFalse(
            self.queue._r.exists('%s:%s:%s' % (
                self.queue._key_prefix, self._test_queue_type, self._test_queue_id)))

    def test_finish_time_keeper_expire(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        # convert to seconds.
        time.sleep(self.queue._job_expire_interval / 1000.00)
        time_keeper_key_name = '%s:%s:%s:time' % (
            self.queue._key_prefix,
            self._test_queue_type, self._test_queue_id)
        self.assertFalse(self.queue._r.exists(time_keeper_key_name))

    def test_finish_queue_type_ready_set_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        queue_type_ready_set = self.queue._r.smembers(
            '%s:ready:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 0)

    def test_finish_queue_type_active_set_existence(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.finish(
            job_id=job_id,
            queue_id=response['queue_id'],
            queue_type=self._test_queue_type
        )

        queue_type_active_set = self.queue._r.smembers(
            '%s:active:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_active_set), 0)

    def test_requeue_active_sorted_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        self.assertFalse(
            self.queue._r.exists('%s:%s:active' % (
                self.queue._key_prefix, self._test_queue_type)))

    def test_requeue_queue_type_ready_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        queue_type_ready_set = self.queue._r.smembers(
            '%s:ready:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_ready_set), 1)
        self.assertEqual(queue_type_ready_set.pop(), self._test_queue_type)

    def test_requeue_queue_type_active_set(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        queue_type_active_set = self.queue._r.smembers(
            '%s:active:queue_type' % self.queue._key_prefix)
        self.assertEqual(len(queue_type_active_set), 0)

    def test_requeue_requeue_limit_5(self):
        # with requeue limit as 5
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_5
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_5)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_5 - 1)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_5 - 2)

    def test_requeue_requeue_limit_0(self):
        # with requeue limit as 0
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_0
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_0)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(response['status'], 'failure')

    def test_requeue_requeue_limit_neg_1(self):
        # with requeue limit as -1 (requeue infinitely)
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
            requeue_limit=self._test_requeue_limit_neg_1
        )

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_neg_1)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_neg_1)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_neg_1)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        # dequeue from the queue_type
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_neg_1)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

        # requeue the job
        self.queue.requeue()

        self.assertEqual(
            response['requeues_remaining'], self._test_requeue_limit_neg_1)

        # wait until the job expires
        time.sleep(self.queue._job_expire_interval / 1000.00)

    def test_interval_non_existent_queue(self):
        response = self.queue.interval(
            interval=1000,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type
        )
        self.assertEqual(response['status'], 'failure')

        interval_map_name = '%s:interval' % (self.queue._key_prefix)
        # check if interval map exists
        self.assertFalse(self.queue._r.exists(interval_map_name))

    def test_interval_existent_queue(self):
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        # check if interval is saved in the appropriate structure
        interval_map_name = '%s:interval' % (self.queue._key_prefix)
        # check if interval map exists
        self.assertTrue(self.queue._r.exists(interval_map_name))

        # check the value
        interval_map_key = '%s:%s' % (
            self._test_queue_type, self._test_queue_id)
        interval = self.queue._r.hget(interval_map_name, interval_map_key)
        self.assertEqual(interval, '10000')

        # set the interval to 5s (5000ms)
        response = self.queue.interval(
            interval=5000,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type
        )
        self.assertEqual(response['status'], 'success')

        # check if interval is saved in the appropriate structure
        interval_map_name = '%s:interval' % (self.queue._key_prefix)
        # check if interval map exists
        self.assertTrue(self.queue._r.exists(interval_map_name))

        # check the value
        # check the value
        interval_map_key = '%s:%s' % (
            self._test_queue_type, self._test_queue_id)
        interval = self.queue._r.hget(interval_map_name, interval_map_key)
        self.assertEqual(interval, '5000')

    def test_metrics_response_status(self):
        response = self.queue.metrics()
        self.assertEqual(response['status'], 'success')

        response = self.queue.metrics(self._test_queue_type)
        self.assertEqual(response['status'], 'success')

        response = self.queue.metrics(
            self._test_queue_type, self._test_queue_id)
        self.assertEqual(response['status'], 'success')

    def test_metrics_response_queue_types(self):
        response = self.queue.metrics()
        self.assertEqual(response['queue_types'], [])
        self.assertEqual(len(response['enqueue_counts'].values()), 10)
        self.assertEqual(sum(response['enqueue_counts'].values()), 0)
        self.assertEqual(len(response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(response['dequeue_counts'].values()), 0)
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = self.queue.metrics()
        self.assertEqual(response['queue_types'], [self._test_queue_type])
        self.assertEqual(len(response['enqueue_counts'].values()), 10)
        self.assertEqual(sum(response['enqueue_counts'].values()), 1)
        self.assertEqual(len(response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(response['dequeue_counts'].values()), 0)

        response = self.queue.dequeue(queue_type=self._test_queue_type)
        response = self.queue.metrics()
        self.assertEqual(len(response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(response['dequeue_counts'].values()), 1)

    def test_metrics_response_queue_ids(self):
        response = self.queue.metrics(queue_type=self._test_queue_type)
        self.assertEqual(response['queue_ids'], [])
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = self.queue.metrics(queue_type=self._test_queue_type)
        self.assertEqual(response['queue_ids'], [self._test_queue_id])

        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )
        response = self.queue.metrics(queue_type=self._test_queue_type)
        self.assertEqual(response['queue_ids'], [self._test_queue_id])

    def test_metrics_response_enqueue_counts_list(self):
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(len(response['enqueue_counts'].values()), 10)
        self.assertEqual(sum(response['enqueue_counts'].values()), 0)
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(len(response['enqueue_counts'].values()), 10)
        self.assertEqual(sum(response['enqueue_counts'].values()), 1)

    def test_metrics_response_dequeue_counts_list(self):
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(len(response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(response['dequeue_counts'].values()), 0)

        response = self.queue.dequeue(queue_type=self._test_queue_type)
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(len(response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(response['dequeue_counts'].values()), 0)

        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = self.queue.dequeue(queue_type=self._test_queue_type)
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(len(response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(response['dequeue_counts'].values()), 1)

    def test_metrics_response_queue_length(self):
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(response['queue_length'], 0)

        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(response['queue_length'], 1)

        response = self.queue.dequeue(queue_type=self._test_queue_type)
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        self.assertEqual(response['queue_length'], 0)

    def test_metrics_enqueue_sliding_window(self):
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(len(response['enqueue_counts'].values()), 10)
        self.assertEqual(sum(response['enqueue_counts'].values()), 0)
        self.assertEqual(len(global_response['enqueue_counts'].values()), 10)
        self.assertEqual(sum(global_response['enqueue_counts'].values()), 0)

        # enqueue a job
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['enqueue_counts'][timestamp_minute], 1)
        self.assertEqual(
            global_response['enqueue_counts'][timestamp_minute], 1)

        # enqueue another job
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['enqueue_counts'][timestamp_minute], 2)
        self.assertEqual(
            global_response['enqueue_counts'][timestamp_minute], 2)

        # wait for one minute
        time.sleep(65)  # 65 seconds
        # check the last minute value.
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['enqueue_counts'][timestamp_minute], 2)
        self.assertEqual(
            global_response['enqueue_counts'][timestamp_minute], 2)

        # save the old value before overwriting
        old_1_timestamp_minute = timestamp_minute
        # check the current minute value
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['enqueue_counts'][timestamp_minute], 0)
        self.assertEqual(
            global_response['enqueue_counts'][timestamp_minute], 0)

        # enqueue a job in the current minute
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['enqueue_counts'][timestamp_minute], 1)
        self.assertEqual(response['enqueue_counts'][old_1_timestamp_minute], 2)
        self.assertEqual(
            global_response['enqueue_counts'][timestamp_minute], 1)
        self.assertEqual(
            global_response['enqueue_counts'][old_1_timestamp_minute], 2)

        time.sleep(65)  # sleep for another 65s

        # save the old timestamp
        old_2_timestamp_minute = timestamp_minute
        # check the current minute value
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['enqueue_counts'][timestamp_minute], 0)
        self.assertEqual(
            global_response['enqueue_counts'][timestamp_minute], 0)

        # enqueue a job in the current minute
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=10000,  # 10s (10000ms)
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['enqueue_counts'][timestamp_minute], 1)
        self.assertEqual(response['enqueue_counts'][old_1_timestamp_minute], 2)
        self.assertEqual(response['enqueue_counts'][old_2_timestamp_minute], 1)
        self.assertEqual(
            global_response['enqueue_counts'][timestamp_minute], 1)
        self.assertEqual(
            global_response['enqueue_counts'][old_1_timestamp_minute], 2)
        self.assertEqual(
            global_response['enqueue_counts'][old_2_timestamp_minute], 1)

    def test_metrics_dequeue_sliding_window(self):
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(len(response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(response['dequeue_counts'].values()), 0)
        self.assertEqual(len(global_response['dequeue_counts'].values()), 10)
        self.assertEqual(sum(global_response['dequeue_counts'].values()), 0)

        # enqueue a job
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,  # 100ms
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['dequeue_counts'][timestamp_minute], 1)
        self.assertEqual(
            global_response['dequeue_counts'][timestamp_minute], 1)

        # enqueue another job
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,  # 100ms
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        time.sleep(0.1)  # 100ms

        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['dequeue_counts'][timestamp_minute], 2)
        self.assertEqual(
            global_response['dequeue_counts'][timestamp_minute], 2)

        # wait for one minute
        time.sleep(65)  # 65 seconds
        # check the last minute value.
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['dequeue_counts'][timestamp_minute], 2)
        self.assertEqual(
            global_response['dequeue_counts'][timestamp_minute], 2)

        # save the old value before overwriting
        old_1_timestamp_minute = timestamp_minute
        # check the current minute value
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['dequeue_counts'][timestamp_minute], 0)
        self.assertEqual(
            global_response['dequeue_counts'][timestamp_minute], 0)

        # enqueue a job in the current minute
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,  # 100ms
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        time.sleep(0.1)  # 100ms

        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['dequeue_counts'][timestamp_minute], 1)
        self.assertEqual(response['dequeue_counts'][old_1_timestamp_minute], 2)
        self.assertEqual(
            global_response['dequeue_counts'][timestamp_minute], 1)
        self.assertEqual(
            global_response['dequeue_counts'][old_1_timestamp_minute], 2)

        time.sleep(65)  # sleep for another 65s

        # save the old timestamp
        old_2_timestamp_minute = timestamp_minute
        # check the current minute value
        timestamp = int(generate_epoch())
        # epoch for the minute.
        timestamp_minute = str(int(math.floor(timestamp / 60000.0) * 60000))
        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['dequeue_counts'][timestamp_minute], 0)
        self.assertEqual(
            global_response['dequeue_counts'][timestamp_minute], 0)

        # enqueue a job in the current minute
        job_id = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=100,  # 100ms
            job_id=job_id,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type,
        )

        time.sleep(0.1)  # 100ms

        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        response = self.queue.metrics(
            queue_type=self._test_queue_type, queue_id=self._test_queue_id)
        global_response = self.queue.metrics()
        self.assertEqual(response['dequeue_counts'][timestamp_minute], 1)
        self.assertEqual(response['dequeue_counts'][old_1_timestamp_minute], 2)
        self.assertEqual(response['dequeue_counts'][old_2_timestamp_minute], 1)
        self.assertEqual(
            global_response['dequeue_counts'][timestamp_minute], 1)
        self.assertEqual(
            global_response['dequeue_counts'][old_1_timestamp_minute], 2)
        self.assertEqual(
            global_response['dequeue_counts'][old_2_timestamp_minute], 1)

    def test_sharq_rate_limiting(self):
        job_id_1 = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_1,
            interval=2000,  # 2s (2000ms)
            job_id=job_id_1,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type
        )

        job_id_2 = self._get_job_id()
        response = self.queue.enqueue(
            payload=self._test_payload_2,
            interval=2000,  # 2s (2000ms)
            job_id=job_id_2,
            queue_id=self._test_queue_id,
            queue_type=self._test_queue_type
        )

        # try to do  back-to-back dequeues.
        # only the first one should return the job,
        # the second one should fail and should only
        # succeed after waiting for the time
        # interval specified.
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # check all the responses
        self.assertEqual(response['status'], 'success')
        self.assertEqual(response['queue_id'], self._test_queue_id)
        self.assertEqual(response['job_id'], job_id_1)
        self.assertEqual(response['payload'], self._test_payload_1)

        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        self.assertEqual(response['status'], 'failure')

        time.sleep(2)  # 2s

        # dequeue again
        response = self.queue.dequeue(
            queue_type=self._test_queue_type
        )

        # check all the responses
        self.assertEqual(response['status'], 'success')
        self.assertEqual(response['queue_id'], self._test_queue_id)
        self.assertEqual(response['job_id'], job_id_2)
        self.assertEqual(response['payload'], self._test_payload_2)

    def tearDown(self):
        # flush all the keys in the test db after each test
        self.queue._r.flushdb()


def main():
    unittest.main()

if __name__ == '__main__':
    main()
