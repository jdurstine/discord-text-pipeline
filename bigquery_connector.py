from google.cloud import bigquery

class bigquery_connector:
    
    def __init__(self, project, environment):
        self.project = project
        self.env = environment
        self.client = bigquery.Client(project)

    def insert_message(self, message_dict):
        table_id = f"{self.project}.{self.env}_raw.raw_messages"
        errors = self.client.insert_rows_json(table_id, [message_dict])
        print(errors)

    def select_messages(self, userid):
        query = f"""
            SELECT msg_content
            FROM {self.project}.{self.env}_raw.raw_messages
            WHERE user_id={userid}"""
        return self.client.query(query).result()