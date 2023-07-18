from google.cloud import bigquery

class bigquery_connector:
    
    def __init__(self, project):
        self.project = project
        self.client = bigquery.Client(project)

    def insert_message(self, message_dict):
        table_id = f"{self.project}.messages.messages"
        errors = self.client.insert_rows_json(table_id, [message_dict])
        print(errors)