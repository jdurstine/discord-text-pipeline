from datetime import datetime
from google.cloud import bigquery

from utils import dt_as_utc_str

class bigquery_connector:
    
    def __init__(self, project, environment):
        self.project = project
        self.env = environment
        self.client = bigquery.Client(project)

    def insert_message(self, message_dict):
        table_id = f"{self.project}.{self.env}_raw.raw_messages"
        errors = self.client.insert_rows_json(table_id, [message_dict])
        if len(errors) > 0:
            raise RuntimeError("Issue encountered inserting message data.")

    def select_messages(self, userid, start = 'NULL', end = 'NULL', order='desc'):

        if type(start) is not datetime and start != 'NULL':
            raise Exception('start was not supplied with a valid argument.')
        
        if type(end) is not datetime and end != 'NULL':
            raise Exception('end was not supplied with a valid argument.')

        if order not in ('asc', 'desc'):
            raise Exception('order was not supplied with a valid argument.')

        if start != 'NULL':
            start = f"'{dt_as_utc_str(start)}'"

        if end != 'NULL':
            end = f"'{dt_as_utc_str(end)}'"

        query = f"""
            SELECT msg_content
            FROM {self.project}.{self.env}_raw.raw_messages
            WHERE user_id={userid}
                AND msg_created_dt BETWEEN 
                    coalesce(datetime({start}), datetime('1990-01-01'))
                    AND coalesce(datetime({end}), datetime('9999-01-01'))
            ORDER BY etl_dt {order}"""
        
        return self.client.query(query).result()
    
    def insert_voice_activity(self, voice_activity_dict):
        table_id = f"{self.project}.{self.env}_raw.raw_voice_activity"
        errors = self.client.insert_rows_json(table_id, [voice_activity_dict])
        if len(errors) > 0:
            raise RuntimeError("Issue encountered inserting voice activity data.")

    def insert_channel(self, channel_data_dict):
        table_id = f"{self.project}.{self.env}_raw.raw_channels"
        errors = self.client.insert_rows_json(table_id, [channel_data_dict])
        if len(errors) > 0:
            raise RuntimeError("Issue encountered inserting channel data.")
    
    def insert_member(self, member_data_dict):
        table_id = f"{self.project}.{self.env}_raw.raw_members"
        errors = self.client.insert_rows_json(table_id, [member_data_dict])
        if len(errors) > 0:
            raise RuntimeError("Issue encountered inserting member data.")
                       
    def latest_message_dts(self):
        query = f"""
            SELECT channel_id, max(msg_created_dt) as max_dt
            FROM {self.project}.{self.env}_raw.raw_messages
            GROUP BY channel_id"""
        result = {row.channel_id:row.max_dt for row in self.client.query(query).result()}
        return result