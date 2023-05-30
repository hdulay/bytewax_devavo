#!/usr/bin/env python
# coding: utf-8

###################
# ---IMPORTANT--- #
###################
# To run this example you'll need a Kafka (or redpanda) cluster.
# Create a topic with using the `create_temperature_events` function
# in examples/example_utils/topics_helper.py:
#
# ```python
# from utils.topics_helper import create_temperature_events
# create_temperature_events("sensors")
# ```
#
# The events generated in the stream will be a json string with 3 keys:
# - type: a string representing the type of reading (eg: "temp")
# - value: a float representing the value of the reading
# - time: A string representing the UTC datetime the event was generated,
#         in isoformat (eg: datetime.now(timezone.utc).isoformat() )
import json
from datetime import datetime, timedelta, timezone

from bytewax.connectors.kafka import KafkaInput
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.window import EventClockConfig, TumblingWindow
from bytewax.inputs import PartitionedInput, StatefulSource

import sseclient
import urllib3


class WikiSource(StatefulSource):
    def __init__(self):
        pool = urllib3.PoolManager()
        resp = pool.request(
            "GET",
            "https://stream.wikimedia.org/v2/stream/recentchange/",
            preload_content=False,
            headers={"Accept": "text/event-stream"},
        )
        self.client = sseclient.SSEClient(resp)
        self.events = self.client.events()

    def next(self):
        return next(self.events).data

    def snapshot(self):
        return None

    def close(self):
        self.client.close()


class WikiStreamInput(PartitionedInput):
    def list_parts(self):
        return {"single-part"}

    def build_part(self, for_key, resume_state):
        assert for_key == "single-part"
        assert resume_state is None
        return WikiSource()
    

# Define the dataflow object and kafka input.
flow = Dataflow()
flow.input("inp", WikiStreamInput())
flow.map(json.loads)

# Divide the readings by sensor type, so that we only
# aggregate readings of the same type.
def extract_sensor_type(event):
    return event['user'], event

flow.map(extract_sensor_type)

# Here is where we use the event time processing, with
# the fold_window operator.
# The `EventClockConfig` allows us to advance our internal clock
# based on the time received in each event.


# This is the accumulator function, and outputs a list of 2-tuples,
# containing the event's "value" and it's "time" (used later to print info)
def acc_values(acc, event):
    acc.append((event["title"], event["timestamp"]))
    return acc


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(event):
    return datetime.fromtimestamp(event["timestamp"], tz=timezone.utc)


# Configure the `fold_window` operator to use the event time.
cc = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=10))

# And a 5 seconds tumbling window
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
wc = TumblingWindow(align_to=align_to, length=timedelta(seconds=5))

flow.fold_window("running_average", cc, wc, list, acc_values)


def is_malicious(event):
    '''
    This checks to see if the pages updated per second exceeds 10pages/sec.
    If it does, the bot is considered malicious and returns a tuple of the
    event and is_malicious.
    '''
    key, data = event
    dates = [datetime.fromtimestamp(x[1]) for x in data]
    delta = max(dates) - min(dates)
    count = len(data)
    sec = 1 if delta.total_seconds()==0 else delta.total_seconds()
    is_malicious = True if count/sec > 10 else False
    return event, is_malicious

# Calculate the average of the values for each window, and
# format the data to a string
def format(event__mal):
    event, mal = event__mal
    key, data = event
    # values = [x[0] for x in data]
    dates = [datetime.fromtimestamp(x[1]) for x in data]
    delta = max(dates) - min(dates)
    count = len(data)
    return (
        f"{key} updated {count} pages in {delta} seconds {'and is malicious!!!!' if mal else ''}"
    )

def f(event):
    return len(event[1]) > 1

flow.filter(f)
flow.map(is_malicious)
flow.map(format)
flow.output("out", StdOutput())

