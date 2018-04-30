from generator import special_words
from google.cloud import bigquery


def query_standard_sql(query):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries are treated as standard SQL by default.
    job_config.use_legacy_sql = False
    client.query(query, job_config=job_config)


def batch(messages):
    query_standard_sql(query)
