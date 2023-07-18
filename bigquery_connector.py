from google.cloud import bigquery

class bigquery_connector:
    
    def __init__(self, project):
        self.project = project
        self.client = bigquery.Client(project)

    def insert_message(self, message_dict):
        # query = f"""
        #     INSERT INTO {self.project}.messages.messages
        #     VALUE {message_dict['etl_dt']},
        #         {message_dict['message_id']},
        #         {message_dict['created_dt']},
        #         {message_dict['user_id']},
        #         {message_dict['referenced_message_id']},
        #         {message_dict['content']}
        # """
        # job = self.client.query(query)
        # job.run()
        table_id = f"{self.project}.messages.messages"
        errors = self.client.insert_rows_json(table_id, [message_dict])
        print(errors)