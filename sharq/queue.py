# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
import os
import sys
import signal
import configparser
import redis
from rediscluster import RedisCluster as StrictRedisCluster
from sharq.utils import (is_valid_identifier, is_valid_interval,
                         is_valid_requeue_limit, generate_epoch,
                         serialize_payload, deserialize_payload,
                         convert_to_str)
from sharq.exceptions import SharqException, BadArgumentException


class SharQ(object):
    """The SharQ object is the core of this queue.
    SharQ does the following.

        1. Accepts a configuration file.
        2. Initializes the queue.
        3. Exposes functions to interact with the queue.
    """

    def __init__(self, config_path):
        """Construct a SharQ object by doing the following.
            1. Read the configuration path.
            2. Load the config.
            3. Initialized SharQ.
        """
        self.config_path = config_path
        self._load_config()
        self._initialize()

    def _initialize(self):
        """Read the SharQ configuration and set appropriate
        variables. Open a redis connection pool and load all
        the Lua scripts.
        """
        self._key_prefix = self._config.get('redis', 'key_prefix')
        self._job_expire_interval = int(
            self._config.get('sharq', 'job_expire_interval')
        )
        self._default_job_requeue_limit = int(
            self._config.get('sharq', 'default_job_requeue_limit')
        )

        # initalize redis
        redis_connection_type = self._config.get('redis', 'conn_type')
        db = self._config.get('redis', 'db')
        if redis_connection_type == 'unix_sock':
            self._r = redis.StrictRedis(
                db=db,
                unix_socket_path=self._config.get('redis', 'unix_socket_path')
            )
        elif redis_connection_type == 'tcp_sock':
            isclustered = False
            if self._config.has_option('redis', 'clustered'):
                isclustered = self._config.getboolean('redis', 'clustered')

            if isclustered:
                startup_nodes = [{"host": self._config.get('redis', 'host'), "port": self._config.get('redis', 'port')}]
                self._r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=False,
                                             skip_full_coverage_check=True, socket_timeout=5)
            else:
                self._r = redis.StrictRedis(
                    db=db,
                    host=self._config.get('redis', 'host'),
                    port=self._config.get('redis', 'port'),
                    password=self._config.get('redis', 'password')
                )
        self._load_lua_scripts()

    def _load_config(self):
        """Read the configuration file and load it into memory."""
        self._config = configparser.SafeConfigParser()
        self._config.read(self.config_path)

    def redis_client(self):
        return self._r

    def reload_config(self, config_path=None):
        """Reload the configuration from the new config file if provided
        else reload the current config file.
        """
        if config_path:
            self.config_path = config_path
        self._load_config()

    def _load_lua_scripts(self):
        """Loads all lua scripts required by SharQ."""
        # load lua scripts
        lua_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'scripts/lua'
        )
        with open(os.path.join(
                lua_script_path,
                'enqueue.lua'), 'r') as enqueue_file:
            self._lua_enqueue_script = enqueue_file.read()
            self._lua_enqueue = self._r.register_script(
                self._lua_enqueue_script)

        with open(os.path.join(
                lua_script_path,
                'dequeue.lua'), 'r') as dequeue_file:
            self._lua_dequeue_script = dequeue_file.read()
            self._lua_dequeue = self._r.register_script(
                self._lua_dequeue_script)

        with open(os.path.join(
                lua_script_path,
                'finish.lua'), 'r') as finish_file:
            self._lua_finish_script = finish_file.read()
            self._lua_finish = self._r.register_script(self._lua_finish_script)

        with open(os.path.join(
                lua_script_path,
                'interval.lua'), 'r') as interval_file:
            self._lua_interval_script = interval_file.read()
            self._lua_interval = self._r.register_script(
                self._lua_interval_script)

        with open(os.path.join(
                lua_script_path,
                'requeue.lua'), 'r') as requeue_file:
            self._lua_requeue_script = requeue_file.read()
            self._lua_requeue = self._r.register_script(
                self._lua_requeue_script)

        with open(os.path.join(
                lua_script_path,
                'metrics.lua'), 'r') as metrics_file:
            self._lua_metrics_script = metrics_file.read()
            self._lua_metrics = self._r.register_script(
                self._lua_metrics_script)

    def reload_lua_scripts(self):
        """Lets user reload the lua scripts in run time."""
        self._load_lua_scripts()

    def enqueue(self, payload, interval, job_id,
                queue_id, queue_type='default', requeue_limit=None):
        """Enqueues the job into the specified queue_id
        of a particular queue_type
        """
        # validate all the input
        if not is_valid_interval(interval):
            raise BadArgumentException('`interval` has an invalid value.')

        if not is_valid_identifier(job_id):
            raise BadArgumentException('`job_id` has an invalid value.')

        if not is_valid_identifier(queue_id):
            raise BadArgumentException('`queue_id` has an invalid value.')

        if not is_valid_identifier(queue_type):
            raise BadArgumentException('`queue_type` has an invalid value.')

        if requeue_limit is None:
            requeue_limit = self._default_job_requeue_limit

        if not is_valid_requeue_limit(requeue_limit):
            raise BadArgumentException('`requeue_limit` has an invalid value.')

        try:
            serialized_payload = serialize_payload(payload)
        except TypeError as e:
            raise BadArgumentException(e.message)

        timestamp = str(generate_epoch())

        keys = [
            self._key_prefix,
            queue_type
        ]

        args = [
            timestamp,
            queue_id,
            job_id,
            serialized_payload,
            interval,
            requeue_limit
        ]
        self._lua_enqueue(keys=keys, args=args)

        response = {
            'status': 'queued'
        }
        return response

    def dequeue(self, queue_type='default'):
        """Dequeues a job from any of the ready queues
        based on the queue_type. If no job is ready,
        returns a failure status.
        """
        if not is_valid_identifier(queue_type):
            raise BadArgumentException('`queue_type` has an invalid value.')

        timestamp = str(generate_epoch())

        keys = [
            self._key_prefix,
            queue_type
        ]
        args = [
            timestamp,
            self._job_expire_interval
        ]

        dequeue_response = self._lua_dequeue(keys=keys, args=args)

        if len(dequeue_response) < 4:
            response = {
                'status': 'failure'
            }
            return response

        queue_id, job_id, payload, requeues_remaining = dequeue_response

        if payload is None:
            response = {
                'status': 'failure'
            }
            return response

        payload = deserialize_payload(payload)

        response = {
            'status': 'success',
            'queue_id': queue_id.decode('utf-8'),
            'job_id': job_id.decode('utf-8'),
            'payload': payload,
            'requeues_remaining': int(requeues_remaining)
        }

        return response

    def finish(self, job_id, queue_id, queue_type='default'):
        """Marks any dequeued job as *completed successfully*.
        Any job which gets a finish will be treated as complete
        and will be removed from the SharQ.
        """
        if not is_valid_identifier(job_id):
            raise BadArgumentException('`job_id` has an invalid value.')

        if not is_valid_identifier(queue_id):
            raise BadArgumentException('`queue_id` has an invalid value.')

        if not is_valid_identifier(queue_type):
            raise BadArgumentException('`queue_type` has an invalid value.')

        keys = [
            self._key_prefix,
            queue_type
        ]

        args = [
            queue_id,
            job_id
        ]

        response = {
            'status': 'success'
        }

        finish_response = self._lua_finish(keys=keys, args=args)
        if finish_response == 0:
            # the finish failed.
            response.update({
                'status': 'failure'
            })
        return response

    def interval(self, interval, queue_id, queue_type='default'):
        """Updates the interval for a specific queue_id
        of a particular queue type.
        """
        # validate all the input
        if not is_valid_interval(interval):
            raise BadArgumentException('`interval` has an invalid value.')

        if not is_valid_identifier(queue_id):
            raise BadArgumentException('`queue_id` has an invalid value.')

        if not is_valid_identifier(queue_type):
            raise BadArgumentException('`queue_type` has an invalid value.')

        # generate the interval key
        interval_hmap_key = '%s:interval' % self._key_prefix
        interval_queue_key = '%s:%s' % (queue_type, queue_id)
        keys = [
            interval_hmap_key,
            interval_queue_key
        ]

        args = [
            interval
        ]
        interval_response = self._lua_interval(keys=keys, args=args)
        if interval_response == 0:
            # the queue with the id and type does not exist.
            response = {
                'status': 'failure'
            }
        else:
            response = {
                'status': 'success'
            }

        return response

    def requeue(self):
        """Re-queues any expired job (one which does not get an expire
        before the job_expiry_interval) back into their respective queue.
        This function has to be run at specified intervals to ensure the
        expired jobs are re-queued back.
        """
        timestamp = str(generate_epoch())
        # get all queue_types and requeue one by one.
        # not recommended to do this entire process
        # in lua as it might take long and block other
        # enqueues and dequeues.
        active_queue_type_list = self._r.smembers(
            '%s:active:queue_type' % self._key_prefix)
        for queue_type in active_queue_type_list:
            # requeue all expired jobs in all queue types.

            queue_type = queue_type.decode('utf-8')

            keys = [
                self._key_prefix,
                queue_type
            ]

            args = [
                timestamp
            ]
            job_discard_list = self._lua_requeue(keys=keys, args=args)
            # discard the jobs if any
            for job in job_discard_list:
                queue_id, job_id = job.decode('utf-8').split(':')
                # explicitly finishing a job
                # is nothing but discard.
                self.finish(
                    job_id=job_id,
                    queue_id=queue_id,
                    queue_type=queue_type
                )

    def metrics(self, queue_type=None, queue_id=None):
        """Provides a way to get statistics about various parameters like,
        * global enqueue / dequeue rates per min.
        * per queue enqueue / dequeue rates per min.
        * queue length of each queue.
        * list of queue ids for each queue type.
        """
        if queue_id is not None and not is_valid_identifier(queue_id):
            raise BadArgumentException('`queue_id` has an invalid value.')

        if queue_type is not None and not is_valid_identifier(queue_type):
            raise BadArgumentException('`queue_type` has an invalid value.')

        response = {
            'status': 'failure'
        }
        if not queue_type and not queue_id:
            # return global stats.
            # list of active queue types (ready + active)
            active_queue_types = self._r.smembers(
                '%s:active:queue_type' % self._key_prefix)
            ready_queue_types = self._r.smembers(
                '%s:ready:queue_type' % self._key_prefix)
            all_queue_types = active_queue_types | ready_queue_types
            queue_types = convert_to_str(all_queue_types)
            # global rates for past 10 minutes
            timestamp = str(generate_epoch())
            keys = [
                self._key_prefix
            ]
            args = [
                timestamp
            ]
            enqueue_details, dequeue_details = self._lua_metrics(
                keys=keys, args=args)

            enqueue_counts = {}
            dequeue_counts = {}
            # the length of enqueue & dequeue details are always same.
            for i in range(0, len(enqueue_details), 2):
                enqueue_counts[str(enqueue_details[i])] = int(
                    enqueue_details[i + 1] or 0)
                dequeue_counts[str(dequeue_details[i])] = int(
                    dequeue_details[i + 1] or 0)

            response.update({
                'status': 'success',
                'queue_types': queue_types,
                'enqueue_counts': enqueue_counts,
                'dequeue_counts': dequeue_counts
            })
            return response
        elif queue_type and not queue_id:
            # return list of queue_ids.
            # get data from two sorted sets in a transaction
            pipe = self._r.pipeline()
            pipe.zrange('%s:%s' % (self._key_prefix, queue_type), 0, -1)
            pipe.zrange('%s:%s:active' % (self._key_prefix, queue_type), 0, -1)
            ready_queues, active_queues = pipe.execute()
            # extract the queue_ids from the queue_id:job_id string
            active_queues = [i.decode('utf-8').split(':')[0] for i in active_queues]
            all_queue_set = set(ready_queues) | set(active_queues)
            queue_list = convert_to_str(all_queue_set)
            response.update({
                'status': 'success',
                'queue_ids': queue_list
            })
            return response
        elif queue_type and queue_id:
            # return specific details.
            active_queue_types = self._r.smembers(
                '%s:active:queue_type' % self._key_prefix)
            ready_queue_types = self._r.smembers(
                '%s:ready:queue_type' % self._key_prefix)
            all_queue_types = active_queue_types | ready_queue_types
            # queue specific rates for past 10 minutes
            timestamp = str(generate_epoch())
            keys = [
                '%s:%s:%s' % (self._key_prefix, queue_type, queue_id)
            ]
            args = [
                timestamp
            ]
            enqueue_details, dequeue_details = self._lua_metrics(
                keys=keys, args=args)

            enqueue_counts = {}
            dequeue_counts = {}
            # the length of enqueue & dequeue details are always same.
            for i in range(0, len(enqueue_details), 2):
                enqueue_counts[str(enqueue_details[i])] = int(
                    enqueue_details[i + 1] or 0)
                dequeue_counts[str(dequeue_details[i])] = int(
                    dequeue_details[i + 1] or 0)

            # get the queue length for the job queue
            queue_length = self._r.llen('%s:%s:%s' % (
                self._key_prefix, queue_type, queue_id))

            response.update({
                'status': 'success',
                'queue_length': int(queue_length),
                'enqueue_counts': enqueue_counts,
                'dequeue_counts': dequeue_counts
            })
            return response
        elif not queue_type and queue_id:
            raise BadArgumentException(
                '`queue_id` should be accompanied by `queue_type`.')

        return response

    def deep_status(self):
        """
        To check the availability of redis. If redis is down get will throw exception
        :return: value or None
        """
        return self._r.set('sharq:deep_status:{}'.format(self._key_prefix), 'sharq_deep_status')

    def clear_queue(self, queue_type=None, queue_id=None, purge_all=False):
        """clear the all entries in queue with particular queue_id
        and queue_type. It takes an optional argument, 
        purge_all : if True, then it will remove the related resources
        from the redis.
        """
        if queue_id is None or not is_valid_identifier(queue_id):
            raise BadArgumentException('`queue_id` has an invalid value.')

        if queue_type is None or not is_valid_identifier(queue_type):
            raise BadArgumentException('`queue_type` has an invalid value.')

        response = {
            'status': 'Failure',
            'message': 'No queued calls found'
        }
        # remove from the primary sorted set
        primary_set = '{}:{}'.format(self._key_prefix, queue_type)
        queued_status = self._r.zrem(primary_set, queue_id)
        if queued_status:
            response.update({'status': 'Success',
                             'message': 'Successfully removed all queued calls'})
        # do a full cleanup of reources
        # although this is not necessary as we don't remove resources 
        # while dequeue operation
        job_queue_list = '{}:{}:{}'.format(self._key_prefix, queue_type, queue_id)
        if queued_status and purge_all:
            job_list = self._r.lrange(job_queue_list, 0, -1)
            pipe = self._r.pipeline()
            # clear the payload data for job_uuid
            for job_uuid in job_list:
                if job_uuid is None:
                    continue
                payload_set = '{}:payload'.format(self._key_prefix)
                job_payload_key = '{}:{}:{}'.format(queue_type, queue_id, job_uuid)
                pipe.hdel(payload_set, job_payload_key)
            # clear jobrequest interval
            interval_set = '{}:interval'.format(self._key_prefix)
            job_interval_key = '{}:{}'.format(queue_type, queue_id)
            pipe.hdel(interval_set, job_interval_key)
            # clear job_queue_list
            pipe.delete(job_queue_list)
            pipe.execute()
            response.update({'status': 'Success',
                             'message': 'Successfully removed all queued calls and purged related resources'})
        else:
            # always delete the job queue list
            self._r.delete(job_queue_list)
        return response
