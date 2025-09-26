import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import csv, json

class ParseCSV(beam.DoFn):
    def process(self, line):
        # Assumes header is skipped upstream. For safety, ignore empty lines.
        if not line.strip():
            return
        parts = line.split(",")
        # Simple CSV parse for demo purposes; production-ready code should be more robust
        if parts[0].lower() == "transaction_id":
            return
        try:
            yield {
                "transaction_id": parts[0],
                "store_id": int(parts[1]),
                "product_id": int(parts[2]),
                "quantity": int(parts[3]),
                "price": float(parts[4]),
                "ts": parts[5]
            }
        except Exception as e:
            yield beam.pvalue.TaggedOutput('bad', {'line': line, 'error': str(e)})

def run(argv=None):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output_bq', required=True)
    parser.add_argument('--temp_location', required=True)
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=options) as p:
        lines = p | 'Read' >> beam.io.ReadFromText(args.input, skip_header_lines=1)
        parsed = lines | 'Parse' >> beam.ParDo(ParseCSV()).with_outputs('bad', main='good')
        good = parsed.good
        bad = parsed.bad

        (good
         | 'WriteToBQ' >> beam.io.WriteToBigQuery(
             args.output_bq,
             schema='transaction_id:STRING,store_id:INTEGER,product_id:INTEGER,quantity:INTEGER,price:NUMERIC,ts:TIMESTAMP',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             custom_gcs_temp_location=args.temp_location
         ))

        (bad
         | 'BadToJSON' >> beam.Map(lambda r: json.dumps(r))
         | 'WriteBad' >> beam.io.WriteToText(args.temp_location + '/deadletters/bad', file_name_suffix='.json'))

if __name__ == '__main__':
    run()
