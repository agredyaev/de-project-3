DROP TABLE IF EXISTS staging.user_activity_log;

CREATE TABLE staging.user_activity_log(
    id serial,
    date_time TIMESTAMP,
    action_id BIGINT,
    customer_id BIGINT,
    quantity BIGINT,
    PRIMARY KEY (id)
);

CREATE INDEX main1 ON staging.user_activity_log (customer_id);