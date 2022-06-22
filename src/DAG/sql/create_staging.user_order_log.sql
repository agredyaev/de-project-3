DROP TABLE IF EXISTS staging.user_order_log;

CREATE TABLE staging.user_order_log(
    id serial,
    date_time TIMESTAMP,
    city_id integer,
    city_name varchar (100),
    customer_id BIGINT,
    first_name varchar (100),
    last_name varchar (100),
    item_id integer,
    item_name varchar(100),
    quantity BIGINT,
    payment_amount numeric(14, 2),
    PRIMARY KEY (id)
);

CREATE INDEX main2 ON staging.user_order_log (customer_id);

ALTER TABLE
    staging.user_order_log
ADD
    status varchar(20) NULL DEFAULT 'shipping';