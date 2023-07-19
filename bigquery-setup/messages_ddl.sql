CREATE TABLE dev_raw.raw_messages (
    etl_dt DATETIME NOT NULL,
    message_id INT NOT NULL,
    created_dt DATETIME NOT NULL,
    user_id INT NOT NULL,
    referenced_message_id INT,
    content STRING NOT NULL
)