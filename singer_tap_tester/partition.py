from collections import defaultdict
import json
import logging


LOG = logging.getLogger(__name__)


def by_type(messages):
    """Given a list of dictionaries, group by the type of the dictionary"""
    tap_output = {
        'RECORD': defaultdict(list),
        'STATE': [],
        'SCHEMA': [],
    }

    for i, message in enumerate(messages):

        message_type = message['type']

        if message_type == 'RECORD':
            stream = message['stream']
            tap_output[message_type][stream].append(message)
        else:
            tap_output[message_type].append(message)

    return tap_output
