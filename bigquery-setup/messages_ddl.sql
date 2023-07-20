CREATE TABLE dev_raw.raw_messages (
    etl_dt DATETIME NOT NULL,
    msg_id INT NOT NULL,
    msg_created_dt DATETIME NOT NULL,
    channel_id INT NOT NULL,
    channel_name STRING NOT NULL,
    user_id INT NOT NULL,
    user_global_name STRING,
    user_display_name STRING,
    user_nickname STRING,
    user_name STRING,
    ref_msg_id INT,
    msg_content STRING NOT NULL
)