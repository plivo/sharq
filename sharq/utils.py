# -*- coding: utf-8 -*-
# Copyright (c) 2014 Plivo Team. See LICENSE.txt for details.
import time
import msgpack

VALID_IDENTIFIER_SET = set(list('abcdefghijklmnopqrstuvwxyz0123456789_-'))


def is_valid_identifier(identifier):
    """Checks if the given identifier is valid or not. A valid
    identifier may consists of the following characters with a
    maximum length of 100 characters, minimum of 1 character.

    Valid characters for an identifier,
        - A to Z
        - a to z
        - 0 to 9
        - _ (underscore)
        - - (hypen)
    """
    if not isinstance(identifier, basestring):
        return False

    if len(identifier) > 100 or len(identifier) < 1:
        return False

    condensed_form = set(list(identifier.lower()))
    return condensed_form.issubset(VALID_IDENTIFIER_SET)


def is_valid_interval(interval):
    """Checks if the given interval is valid. A valid interval
    is always a positive, non-zero integer value.
    """
    if not isinstance(interval, (int, long)):
        return False

    if interval <= 0:
        return False

    return True


def is_valid_requeue_limit(requeue_limit):
    """Checks if the given requeue limit is valid.
    A valid requeue limit is always greater than
    or equal to -1.
    """
    if not isinstance(requeue_limit, (int, long)):
        return False

    if requeue_limit <= -2:
        return False

    return True


def serialize_payload(payload):
    """Tries to serialize the payload using msgpack. If it is
    not serializable, raises a TypeError.
    """
    return msgpack.packb(payload)


def deserialize_payload(payload):
    """Tries to deserialize the payload using msgpack.
    """
    return msgpack.unpackb(payload)


def generate_epoch():
    """Generates an unix epoch in ms.
    """
    return int(time.time() * 1000)
