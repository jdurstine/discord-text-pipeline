from google.cloud import bigquery

class bigquery_connector:
    
    def __init__(self, project, environment):
        self.project = project
        self.env = environment
        self.client = bigquery.Client(project)

    def insert_message(self, message_dict):
        table_id = f"{self.project}.{self.env}_raw.raw_messages"
        errors = self.client.insert_rows_json(table_id, [message_dict])
        if len(errors) > 0:
            print(errors)

    def select_messages(self, userid, order='desc'):
        query = f"""
            SELECT msg_content
            FROM {self.project}.{self.env}_raw.raw_messages
            WHERE user_id={userid}
            ORDER BY etl_dt {order}"""
        return self.client.query(query).result()
    
    def latest_message_dts(self):
        query = f"""
            SELECT channel_id, max(msg_created_dt) as max_dt
            FROM {self.project}.{self.env}_raw.raw_messages
            GROUP BY channel_id"""
        result = {row.channel_id:row.max_dt for row in self.client.query(query).result()}
        return result