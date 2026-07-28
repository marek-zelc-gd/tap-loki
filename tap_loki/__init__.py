#!/usr/bin/env python3

from datetime import datetime
from copy import deepcopy
import pytz
import os
import json
import requests

import singer
from singer import utils, Transformer
from singer import metadata

from promalyze import Client
from promalyze.timeseries import TimeSeries
from promalyze.vector import Vector
from promalyze.prometheus_data import PrometheusData
from promalyze.utils import time_to_epoch
from datetime import datetime, timedelta, timezone



REQUIRED_CONFIG_KEYS = ['endpoint', 'start_date', 'metrics']
STATE = {}

LOGGER = singer.get_logger()

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

class CustomClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _handle_results(self,data):
        results = PrometheusData()

        if data['resultType'] == 'vector':
            for d in data['result']:
                results.vectors.append(Vector(d))
        if data['resultType'] == 'streams':
            for d in data['result']:
                ts = TimeSeries(d['stream'], d['values'])
                results.timeseries.append(ts)
        if data['resultType'] == 'matrix':
            for d in data['result']:
                ts = TimeSeries(d['metric'], d['values'])
                results.timeseries.append(ts)

        return results
    
    def _fetch(self, resource, params=None):
        if params is None:
            params = {}

        res = requests.get(self._full_url(resource), params, auth=self._auth, headers={"X-Scope-OrgID": "fake"})
        #print(res.json())
        return res.json()
    def range_query(self, metric, start=None, end=None, step=5, params=None):
        """
        Returns a PrometheusData object loaded with results from a query_range call
        :param metric: string of the metric query
        :param start: an epoch time for the earliest data point in the range -- default is 24 hours ago
        :param end: an epoch time for the latest data point in the range -- default is the current time
        :param step: the step size
        :param params: additional params to send to the API
        :return: PrometheusData
        """
        if params is None:
            params = {}

        params['query'] = metric
        params['step'] = step
        params['limit'] = 10000

        if start is None:
            start = time_to_epoch((datetime.now() - timedelta(days=1)))

        if end is None:
            end = time_to_epoch(datetime.now())

        params['start'] = datetime.fromtimestamp(start, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        params['end'] = datetime.fromtimestamp(end, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        response = self._fetch('query_range', params)
        data = self._extract_data(response)
        return self._handle_results(data)
    
class Context:
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}
    new_counts = {}
    updated_counts = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map.get(stream_name)

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"]
                  if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if stream is not None:
            stream_metadata = metadata.to_map(stream['metadata'])
            return metadata.get(stream_metadata, (), 'selected')
        return False

    @classmethod
    def print_counts(cls):
        LOGGER.info('------------------')
        for stream_name, stream_count in Context.new_counts.items():
            LOGGER.info('%s: %d new, %d updates',
                        stream_name,
                        stream_count,
                        Context.updated_counts[stream_name])
        LOGGER.info('------------------')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schema():
    filename = "aggregated_metric_history.json"
    path = get_abs_path('schemas') + '/' + filename
    with open(path) as file:
        schema: dict = json.load(file)
        return schema


def discover():
    raw_schema = load_schema()
    streams = []

    for metric in Context.config['metrics']:
        # build a schema by merging the raw schema and the metric configuration
        schema = deepcopy(raw_schema)
        schema['properties']['labels'] = metric['labels']

        # create and add catalog entry
        catalog_entry = {
            'stream': metric['name'],
            'tap_stream_id': metric['name'],
            'schema': schema,
            # TODO Events may have a different key property than this. Change
            # if it's appropriate.
            'key_properties': ['date', 'labels']
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def sync(client):
    # Write all schemas and init count to 0
    for catalog_entry in Context.catalog['streams']:
        stream_name = catalog_entry["tap_stream_id"]
        singer.write_schema(
            stream_name, catalog_entry['schema'], catalog_entry['key_properties'])

        Context.new_counts[stream_name] = 0
        Context.updated_counts[stream_name] = 0

    for metric in Context.config['metrics']:
        name = metric['name']
        query = metric['query']
        batch = metric['batch']
        step = metric['step']

        LOGGER.info('Loading metric "%s" using query "%s", metric batch: %s, metric step: %s',
                    name, query, batch, step)

        query_metric(client, name, query, batch, step)


def query_metric(client: CustomClient, name: str, query: str, batch: int, step: int):
    """Queries a metric from the prometheus API and writes singer records

    Args:
        client: Prometheus API client
        name: name of the metric
        query: PromQL query
        batch: The maximum batch size, i.e. how many steps to fetch at once
        step: The step size in seconds
    """
    stream_name = name  # the stream has the same name as the metric in the config
    catalog_entry = Context.get_catalog_entry(stream_name)
    stream_schema = catalog_entry['schema']

    bookmark = get_bookmark(name)
    bookmark_unixtime = int(datetime.strptime(
        bookmark, DATE_FORMAT).replace(tzinfo=pytz.UTC).timestamp())

    LOGGER.info(f'Stream {stream_name}: loaded bookmark @ {bookmark_unixtime}')

    extraction_time = singer.utils.now()
    current_unixtime = int(extraction_time.timestamp())

    # we always start at the configured start_date and ever advance collection by multiple of step
    # so we should never lose alignment
    fetch_steps = int((current_unixtime - bookmark_unixtime) / step)
    iterator_unixtime = bookmark_unixtime
    
    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:

        synced_steps = 0
        while synced_steps < fetch_steps:
            batch_steps = min(batch, fetch_steps - synced_steps)
            next_iterator_unixtime = iterator_unixtime + (batch_steps * step)
            
            LOGGER.info(f'Stream {stream_name}: fetching a batch of {batch_steps} steps @ {step}s, from {iterator_unixtime} to {next_iterator_unixtime}. Already synced {synced_steps}/{fetch_steps} steps of.')
            
            ts_data = client.range_query(
                query,
                start=iterator_unixtime,
                end=next_iterator_unixtime,
                step=step
            )  # returns PrometheusData object

            for ts in ts_data.timeseries:
                labels = ts.metadata

                for x in ts.ts:
                    data = {
                        "date": x[0],
                        "labels": labels,
                        "value": try_parse_float(x[1])
                    }

                    rec = transformer.transform(data, stream_schema)

                    singer.write_record(
                        stream_name,
                        rec,
                        time_extracted=extraction_time
                    )

                    Context.new_counts[stream_name] += 1

            if len(ts_data.timeseries) == 0:
                LOGGER.warn(
                    'Request %s returned an empty result for the date %s', query, iterator_unixtime)

            bookmark_value = datetime.utcfromtimestamp(
                next_iterator_unixtime).strftime(DATE_FORMAT)
            write_bookmark(name, bookmark_value)

            # write state everytime, as batches might be quite large already
            singer.write_state(Context.state)

            synced_steps += batch_steps
            iterator_unixtime = next_iterator_unixtime

    # ensure we write state at least once, e.g. if we did have nothing to collect in an incremental run
    singer.write_state(Context.state)
    

def try_parse_float(element: any):
    if element is None: 
        return None
    try:
        # JSON can't represent NaN and +/-Inf, so we do a quick test for them and return None in that case
        # see https://stackoverflow.com/a/62171968/125407
        x = float(element)
        return None if x != x else x
    except ValueError:
        return None


def get_bookmark(name):
    # Prefer exact stream bookmark lookup.
    bookmark = singer.get_bookmark(Context.state, name, 'start_date')
    if bookmark is None:
        bookmark = singer.get_bookmark(
            Context.state, name, 'replication_key_value')

    # Fallback: some runners may alter stream keys slightly. Try normalized key match.
    if bookmark is None and isinstance(Context.state, dict):
        bookmarks = Context.state.get('bookmarks', {})
        normalized_name = normalize_stream_key(name)
        for stream_key, stream_bookmark in bookmarks.items():
            if normalize_stream_key(stream_key) != normalized_name:
                continue
            if not isinstance(stream_bookmark, dict):
                continue
            bookmark = stream_bookmark.get('start_date') or stream_bookmark.get('replication_key_value')
            if bookmark is not None:
                LOGGER.info(
                    'Bookmark lookup: stream "%s" matched state key "%s"',
                    name,
                    stream_key
                )
                break

    if bookmark is None:
        LOGGER.info(
            'Bookmark lookup: stream "%s" missing in state, falling back to config start_date',
            name
        )
        bookmark = Context.config['start_date']
        LOGGER.info('Stream %s: no saved bookmark, using config start_date %s', name, bookmark)
    else:
        LOGGER.info('Stream %s: using saved bookmark %s', name, bookmark)
    return bookmark


def write_bookmark(stream_name, bookmark_value):
    if not isinstance(Context.state, dict):
        Context.state = {}

    bookmarks = Context.state.setdefault('bookmarks', {})
    stream_bookmark = bookmarks.setdefault(stream_name, {})
    stream_bookmark['start_date'] = bookmark_value
    # Keep Singer SDK compatibility key in sync.
    stream_bookmark['replication_key_value'] = bookmark_value


def normalize_state(raw_state):
    if isinstance(raw_state, str):
        try:
            raw_state = json.loads(raw_state)
            LOGGER.info('State source: parsed state from JSON string input')
        except ValueError:
            LOGGER.info('State source: invalid JSON string state, starting with empty state')
            return {}

    if not isinstance(raw_state, dict):
        LOGGER.info('State source: no valid state object provided, starting with empty state')
        return {}

    # The tap emits state as a Singer STATE message ({"type": "STATE", "value": {...}}).
    # If the state file captured the message verbatim instead of just the inner value,
    # unwrap it so singer.get_bookmark can find the bookmarks.
    if raw_state.get('type') == 'STATE' and isinstance(raw_state.get('value'), dict):
        LOGGER.info('State source: using wrapped Singer state from STATE message envelope')
        return raw_state['value']

    # Some orchestrators wrap Singer state under completed.singer_state.
    completed_state = raw_state.get('completed', {})
    if isinstance(completed_state, dict):
        nested_singer_state = completed_state.get('singer_state')
        if isinstance(nested_singer_state, dict):
            LOGGER.info('State source: using wrapped Singer state from completed.singer_state')
            return nested_singer_state

    # Other wrappers may place singer_state at the top level.
    top_level_singer_state = raw_state.get('singer_state')
    if isinstance(top_level_singer_state, dict):
        LOGGER.info('State source: using wrapped Singer state from top-level singer_state')
        return top_level_singer_state

    LOGGER.info('State source: using provided top-level Singer state')
    return raw_state


def normalize_stream_key(stream_name):
    return str(stream_name).replace('-', '_').replace('/', '.')


def init_prom_client():
    auth = None
    if Context.config['auth']:
        auth = (Context.config['auth']['username'],
                Context.config['auth']['password'])

    return CustomClient(Context.config['endpoint'], auth)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    Context.config = args.config

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))

    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()

        Context.state = normalize_state(args.state)
        LOGGER.info('Loaded state: %s', json.dumps(Context.state))

        client = init_prom_client()
        sync(client)


if __name__ == '__main__':
    main()
