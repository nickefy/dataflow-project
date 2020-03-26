import argparse
import datetime
import json
import ast
import logging
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.utils.timestamp import Duration
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import PubsubMessage

# 2020-03-06 Added method and retry strategy in WriteToBigQuery Beam Step
# 2020-03-06 tried to run without using dataflow template

# 2020-03-08 Added Global Window and Allow late data


# 2020-03-09 Windowed Pcollection into Fixed windows and Allowed_lateness

# table dictionary to identify tables
table_dictionary ={
'columns_table_name' : 'schema',
'keys_table_name' : 'schema',
}

# pardo function to log pubsub messages
class transform_class(beam.DoFn):

    def process(self, element, publish_time=beam.DoFn.TimestampParam, *args, **kwargs):
        logging.info(element)
        yield element

# identify tables from pubsub topic and label them
class identify_and_transform_tables(beam.DoFn):

    def process(self, element, publish_time=beam.DoFn.TimestampParam, table_dictionary = table_dictionary, *arg, **kwargs):
        if (element.data != None and element.data != b'' and element.data != "b''"):
            data = json.loads(element.data)
            data['publish_time'] = (datetime.datetime.utcfromtimestamp(
                    float(publish_time)) + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S.%f")
            if list(data.keys()) == table_dictionary['columns_table_name']:
                data['timestamp'] = datetime.datetime(data['timestamp']['DateTime']['year'], data['timestamp']['DateTime']['month'], data['timestamp']['DateTime']['day'], data['timestamp']['DateTime']['hour'], data['timestamp']['DateTime']['minute'], data['timestamp']['DateTime']['second'], data['timestamp']['DateTime']['micro'])
                data['timestamp'] = data['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')
                yield pvalue.TaggedOutput('table_name', data)
                logging.info('this is table_name' + str(data))
        else:
            keys = element.attributes['key']
            keys = {}
            for attr in element.attributes['key'][7:-1].split(','):
                key, val = attr.split('=')
                try:
                    keys[key] = int(val)
                except ValueError as ve:
                    keys[key] = val

            keys['publish_time'] = (datetime.datetime.utcfromtimestamp(
                    float(publish_time)) + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S.%f")

            if list(keys.keys()) == table_dictionary['keys_table_name']:
                yield pvalue.TaggedOutput('table_name_dbactions', keys)
                logging.info('table_name_dbactions' + str(keys))

def run(pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = (pipeline 
                | 'Read PubSub Messages' >> beam.io.ReadFromPubSub(topic='topic name',with_attributes = True)
                # | 'Windowing into Fixed Windows' >> beam.WindowInto(beam.window.FixedWindows(2 * 60, 0),allowed_lateness=Duration(seconds=2*24*60*60))
                | 'Transforming Messages' >> beam.ParDo(transform_class())
                | 'Identify Tables' >> beam.ParDo(identify_and_transform_tables()).with_outputs('table_name'))

# outputing to bigquery
        table_name = lines.table_name
        table_name = (table_name 
                        # | 'Action States Windowing into Fixed Windows' >> beam.WindowInto(beam.window.FixedWindows(2 * 60, 0),allowed_lateness=Duration(seconds=2*24*60*60))
                        | 'Write Action States to BQ' >> beam.io.gcp.bigquery.WriteToBigQuery(
                        table='table',
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        insert_retry_strategy = 'RETRY_ALWAYS'
                        )
                        )



    result = pipeline.run()

if __name__ == '__main__': # noqa
    run()