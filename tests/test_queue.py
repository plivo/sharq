# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
import os
import unittest
from datetime import date
from sharq import SharQ
from sharq.exceptions import BadArgumentException


class SharQTest(unittest.TestCase):
    """ The SharQTest contains test cases which
    validate the SharQ interface.
    """

    def setUp(self):
        cwd = os.path.dirname(os.path.realpath(__file__))
        config_path = os.path.join(cwd, 'sharq.test.conf')  # test config
        self.queue = SharQ(config_path)

        self.valid_queue_type = '5m5_qu-eue'
        self.invalid_queue_type_1 = 's!ms_queue'
        self.invalid_queue_type_2 = 's!ms queue'
        self.invalid_queue_type_3 = ''

        self.valid_queue_id = 'queue_001-'
        self.invalid_queue_id_1 = 'queue#002'
        self.invalid_queue_id_2 = 'queue 002'
        self.invalid_queue_id_3 = ''

        self.valid_job_id = '96c82500-9f88-11e3-bb98-22000ac6964a'
        self.invalid_job_id_1 = '93 c8'
        self.invalid_job_id_2 = '93)c8'
        self.invalid_job_id_2 = ''

        self.valid_interval = 5000
        self.invalid_interval_1 = '100'
        self.invalid_interval_2 = '$#'
        self.invalid_interval_3 = ''
        self.invalid_interval_4 = 0
        self.invalid_interval_5 = -1

        self.valid_requeue_limit_1 = 5
        self.valid_requeue_limit_2 = 0
        self.valid_requeue_limit_3 = -1
        self.invalid_requeue_limit_1 = '100'
        self.invalid_requeue_limit_2 = '$#'
        self.invalid_requeue_limit_3 = ''
        self.invalid_requeue_limit_4 = -2

        self.valid_payload = {
            'phone_number': '1000000000',
            'message': 'hello world'
        }
        self.invalid_payload = {
            'phone_number': '10000000000',
            'message': 'summer is here!',
            'date': date.today()
        }

        # flush redis before start
        self.queue._r.flushdb()

    def test_enqueue_queue_type_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.invalid_queue_type_1
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.invalid_queue_type_2
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.invalid_queue_type_3
        )

    def test_enqueue_queue_id_missing(self):
        self.assertRaises(
            TypeError,
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            # queue_id is missing
            queue_type=self.valid_queue_type
        )

    def test_enqueue_queue_id_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.invalid_queue_id_1,
            queue_type=self.valid_queue_type
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.invalid_queue_id_2,
            queue_type=self.valid_queue_type
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.invalid_queue_id_3,
            queue_type=self.valid_queue_type
        )

    def test_enqueue_job_id_missing(self):
        self.assertRaises(
            TypeError,
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            # job_id is missing
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

    def test_enqueue_job_id_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`job_id` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.invalid_job_id_1,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`job_id` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.invalid_job_id_2,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

    def test_enqueue_interval_missing(self):
        self.assertRaises(
            TypeError,
            self.queue.enqueue,
            payload=self.valid_payload,
            # interval is missing
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

    def test_enqueue_interval_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.invalid_interval_1,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.invalid_interval_2,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.invalid_interval_3,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        # type 4
        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.invalid_interval_4,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        # type 5
        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.invalid_interval_5,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

    def test_enqueue_requeue_limit_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`requeue_limit` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.invalid_requeue_limit_1
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`requeue_limit` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.invalid_requeue_limit_2
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`requeue_limit` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.invalid_requeue_limit_3
        )

        # type 4
        self.assertRaisesRegexp(
            BadArgumentException,
            '`requeue_limit` has an invalid value.',
            self.queue.enqueue,
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.invalid_requeue_limit_4
        )

    def test_enqueue_cannot_serialize_payload(self):
        self.assertRaisesRegexp(
            BadArgumentException,
            r'can\'t serialize.',
            self.queue.enqueue,
            payload=self.invalid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

    def test_enqueue_all_ok(self):
        # with a queue_type
        response = self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )
        self.assertEqual(response['status'], 'queued')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})

        # without a queue_type (queue_type will be 'default')
        response = self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id
        )
        self.assertEqual(response['status'], 'queued')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})

        # with requeue_limit 1
        response = self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.valid_requeue_limit_1
        )
        self.assertEqual(response['status'], 'queued')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})

        # with requeue_limit 2
        response = self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.valid_requeue_limit_2
        )
        self.assertEqual(response['status'], 'queued')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})

        # with requeue_limit 3
        response = self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type,
            requeue_limit=self.valid_requeue_limit_3
        )
        self.assertEqual(response['status'], 'queued')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})

        # requeue_limit missing
        response = self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )
        self.assertEqual(response['status'], 'queued')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})


    def test_dequeue_queue_type_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.dequeue,
            queue_type=self.invalid_queue_type_1
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.dequeue,
            queue_type=self.invalid_queue_type_2
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.dequeue,
            queue_type=self.invalid_queue_type_3
        )

    def test_dequeue_all_ok(self):
        # first enqueue a job
        self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        # with a queue_type
        response = self.queue.dequeue(
            queue_type=self.valid_queue_type
        )
        self.assertEqual(response['status'], 'success')
        response.pop('status')

        # check if it has a key called 'payload'
        self.assertIn('payload', response)
        response.pop('payload')

        # check if it has a key called 'queue_id'
        self.assertIn('queue_id', response)
        response.pop('queue_id')

        # check if it has a key called 'job_id'
        self.assertIn('job_id', response)
        response.pop('job_id')

        # check if it has a key called 'requeues_remaining'
        self.assertIn('requeues_remaining', response)
        response.pop('requeues_remaining')

        # make sure nothing else in response
        # except the above key / value pairs
        self.assertEqual(response, {})

        # enqueue another job
        self.queue.enqueue(
            payload=self.valid_payload,
            interval=self.valid_interval,
            job_id=self.valid_job_id,
            queue_id=self.valid_queue_id
        )

        # without a queue_type
        response = self.queue.dequeue()
        self.assertEqual(response['status'], 'success')
        response.pop('status')

        # check if it has a key called 'payload'
        self.assertIn('payload', response)
        response.pop('payload')

        # check if it has a key called 'queue_id'
        self.assertIn('queue_id', response)
        response.pop('queue_id')

        # check if it has a key called 'job_id'
        self.assertIn('job_id', response)
        response.pop('job_id')

        # check if it has a key called 'requeues_remaining'
        self.assertIn('requeues_remaining', response)
        response.pop('requeues_remaining')

        # make sure nothing else in response
        # except the above key / value pairs
        self.assertEqual(response, {})

    def test_finish_queue_type_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.finish,
            queue_type=self.invalid_queue_type_1,
            queue_id=self.valid_queue_id,
            job_id=self.valid_job_id
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.finish,
            queue_type=self.invalid_queue_type_2,
            queue_id=self.valid_queue_id,
            job_id=self.valid_job_id
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.finish,
            queue_type=self.invalid_queue_type_3,
            queue_id=self.valid_queue_id,
            job_id=self.valid_job_id
        )

    def test_finish_queue_id_missing(self):
        self.assertRaises(
            TypeError,
            self.queue.finish,
            queue_type=self.valid_queue_type,
            # queue_id missing
            job_id=self.valid_job_id
        )

    def test_finish_queue_id_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.finish,
            queue_type=self.valid_queue_type,
            queue_id=self.invalid_queue_id_1,
            job_id=self.valid_job_id
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.finish,
            queue_type=self.valid_queue_type,
            queue_id=self.invalid_queue_id_2,
            job_id=self.valid_job_id
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.finish,
            queue_type=self.valid_queue_type,
            queue_id=self.invalid_queue_id_3,
            job_id=self.valid_job_id
        )

    def test_finish_job_id_missing(self):
        self.assertRaises(
            TypeError,
            self.queue.finish,
            queue_type=self.valid_queue_type,
            queue_id=self.valid_queue_id,
            # job_id missing
        )

    def test_finish_job_id_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`job_id` has an invalid value.',
            self.queue.finish,
            queue_type=self.valid_queue_type,
            queue_id=self.valid_queue_id,
            job_id=self.invalid_job_id_1
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`job_id` has an invalid value.',
            self.queue.finish,
            queue_type=self.valid_queue_type,
            queue_id=self.valid_queue_id,
            job_id=self.invalid_job_id_2
        )

    def test_finish_all_ok(self):
        # with a queue_type. no existent job.
        response = self.queue.finish(
            queue_type=self.valid_queue_type,
            queue_id=self.valid_queue_id,
            job_id=self.valid_job_id
        )
        self.assertEqual(response['status'], 'failure')
        response.pop('status')

        # make sure nothing else in response
        # except the above key / value pairs
        self.assertEqual(response, {})

        # without a queue_type
        response = self.queue.finish(
            queue_id=self.valid_queue_id,
            job_id=self.valid_job_id
        )
        self.assertEqual(response['status'], 'failure')
        response.pop('status')

        # make sure nothing else in response
        # except the above key / value pairs
        self.assertEqual(response, {})

    def test_interval_interval_invalid(self):
        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.interval,
            interval=self.invalid_interval_1,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.interval,
            interval=self.invalid_interval_2,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.interval,
            interval=self.invalid_interval_3,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.interval,
            interval=self.invalid_interval_4,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`interval` has an invalid value.',
            self.queue.interval,
            interval=self.invalid_interval_5,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

    def test_interval_interval_missing(self):
        self.assertRaises(
            TypeError,
            self.queue.interval,
            # interval parameter missing
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )

    def test_interval_invalid_queue_id(self):
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.interval,
            interval=self.valid_interval,
            queue_id=self.invalid_queue_id_1,
            queue_type=self.valid_queue_type
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.interval,
            interval=self.valid_interval,
            queue_id=self.invalid_queue_id_2,
            queue_type=self.valid_queue_type
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.interval,
            interval=self.valid_interval,
            queue_id=self.invalid_queue_id_3,
            queue_type=self.valid_queue_type
        )

    def test_interval_queue_id_missing(self):
        self.assertRaises(
            TypeError,
            self.queue.interval,
            interval=self.valid_interval,
            # queue_id parameter missing
            queue_type=self.valid_queue_type
        )

    def test_interval_invalid_queue_type(self):
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.interval,
            interval=self.valid_interval,
            queue_id=self.valid_queue_id,
            queue_type=self.invalid_queue_type_1
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.interval,
            interval=self.valid_interval,
            queue_id=self.valid_queue_id,
            queue_type=self.invalid_queue_type_2
        )

        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.interval,
            interval=self.valid_interval,
            queue_id=self.valid_queue_id,
            queue_type=self.invalid_queue_type_3
        )

    def test_interval_all_ok(self):
        # with a queue_type
        response = self.queue.interval(
            interval=self.valid_interval,
            queue_id=self.valid_queue_id,
            queue_type=self.valid_queue_type
        )
        # no queues are found yet.
        self.assertEqual(response['status'], 'failure')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})

        # without a queue_type
        response = self.queue.interval(
            interval=self.valid_interval,
            queue_id=self.valid_queue_id
        )
        # no queues are found yet.
        self.assertEqual(response['status'], 'failure')

        # the result should contain only status
        response.pop('status')
        self.assertEqual(response, {})

    def test_metrics_no_argument(self):
        # with no arguments
        response = self.queue.metrics()
        self.assertEqual(response['status'], 'success')
        response.pop('status')
        # check if it has a key called 'queue_types'
        self.assertIn('queue_types', response)
        response.pop('queue_types')
        # check if it has a key called 'enqueue_counts'
        self.assertIn('enqueue_counts', response)
        response.pop('enqueue_counts')
        # check if it has a key called 'dequeue_counts'
        self.assertIn('dequeue_counts', response)
        response.pop('dequeue_counts')

        self.assertEqual(response, {})

    def test_metrics_only_queue_id(self):
        # with only a valid queue_id
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` should be accompanied by `queue_type`.',
            self.queue.metrics,
            queue_id=self.valid_queue_id,
        )

    def test_metrics_only_queue_type(self):
        # with only a valid queue_type
        response = self.queue.metrics(queue_type=self.valid_queue_type)
        self.assertEqual(response['status'], 'success')
        response.pop('status')
        # check if it has a key called 'queue_ids'
        self.assertIn('queue_ids', response)
        response.pop('queue_ids')

        self.assertEqual(response, {})

    def test_metrics_both_queue_id_queue_type(self):
        # with a valid queue_id and queue_type
        response = self.queue.metrics(
            queue_type=self.valid_queue_type, queue_id=self.valid_queue_id)
        self.assertEqual(response['status'], 'success')
        response.pop('status')
        # check if it has a key called 'queue_length'
        self.assertIn('queue_length', response)
        response.pop('queue_length')
        # check if it has a key called 'enqueue_counts'
        self.assertIn('enqueue_counts', response)
        response.pop('enqueue_counts')
        # check if it has a key called 'dequeue_counts'
        self.assertIn('dequeue_counts', response)
        response.pop('dequeue_counts')

        self.assertEqual(response, {})

    def test_metrics_queue_id_invalid(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.metrics,
            queue_type=self.valid_queue_type,
            queue_id=self.invalid_queue_id_1
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.metrics,
            queue_type=self.valid_queue_type,
            queue_id=self.invalid_queue_id_2
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_id` has an invalid value.',
            self.queue.metrics,
            queue_type=self.valid_queue_type,
            queue_id=self.invalid_queue_id_3
        )

    def test_metrics_invalid_queue_type(self):
        # type 1
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.metrics,
            queue_type=self.invalid_queue_type_1,
            queue_id=self.valid_queue_id
        )

        # type 2
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.metrics,
            queue_type=self.invalid_queue_type_2,
            queue_id=self.valid_queue_id
        )

        # type 3
        self.assertRaisesRegexp(
            BadArgumentException,
            '`queue_type` has an invalid value.',
            self.queue.metrics,
            queue_type=self.invalid_queue_type_3,
            queue_id=self.valid_queue_id
        )

    def tearDown(self):
        # flush redis at the end
        self.queue._r.flushdb()


def main():
    unittest.main()


if __name__ == '__main__':
    main()
