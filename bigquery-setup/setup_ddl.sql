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

CREATE TABLE dev_raw.raw_channels (
    etl_dt DATETIME NOT NULL,
    guild_id INT NOT NULL,
    channel_id INT NOT NULL,
    channel_name STRING NOT NULL,
    channel_status STRING,
    channel_type STRING NOT NULL
)

CREATE TABLE dev_raw.raw_members (
    etl_dt DATETIME NOT NULL,
    guild_id INT NOT NULL,
    member_id INT NOT NULL,
    member_global_name STRING,
    member_display_name STRING,
    member_nickname STRING,
    member_name STRING
)

CREATE TABLE dev_raw.raw_voice_activity (
    etl_dt DATETIME NOT NULL,
    channel_id INT NOT NULL,
    channel_name STRING NOT NULL,
    user_id INT
)