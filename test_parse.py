import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from pipeline.sales_pipeline import ParseCSV

def test_parse_csv():
    input_lines = ["transaction_id,store_id,product_id,quantity,price,timestamp",
                   "1001,101,501,2,30.5,2025-09-20 10:15:00"]
    with TestPipeline() as p:
        out = (p | beam.Create(input_lines) | beam.ParDo(ParseCSV()))
        expected = [{
            "transaction_id": "1001",
            "store_id": 101,
            "product_id": 501,
            "quantity": 2,
            "price": 30.5,
            "ts": "2025-09-20 10:15:00"
        }]
        assert_that(out, equal_to(expected))
