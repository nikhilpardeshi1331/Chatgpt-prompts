class LoadDataToPostgres(beam.DoFn):
    def __init__(self, db_config):
        self.db_config = db_config

    def setup(self):
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        self.bq_client = bigquery.Client()

    def process(self, element):
        table_name, bq_path = element

        with open(bq_path, 'r') as file:
            bq_query = file.read()

        query_job = self.bq_client.query(bq_query)
        results = query_job.result()

        # Dynamically construct the insert statement based on BigQuery query columns
        insert_query = f"INSERT INTO {table_name} ({', '.join([row.name for row in results.schema])}) VALUES ({', '.join(['%s'] * len(results.schema))});"

        # Insert data directly from BigQuery results
        for row in results:
            self.cursor.execute(insert_query, tuple(row))

        self.conn.commit()

    def teardown(self):
        self.cursor.close()
        self.conn.close()
