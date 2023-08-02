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
    
    def latest_message_dt(self, channel_id):
        query = f"""
            SELECT max(msg_created_dt) as max_dt
            FROM {self.project}.{self.env}_raw.raw_messages
            WHERE channel_id = {channel_id}"""
        result = [row for row in self.client.query(query).result()]
        return result[0].max_dt