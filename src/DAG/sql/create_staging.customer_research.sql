DROP TABLE IF EXISTS staging.customer_research;

CREATE TABLE staging.customer_research(
    id serial,
    date_id TIMESTAMP,
    category_id integer,
    geo_id integer,
    sales_qty integer,
    sales_amt numeric(14, 2),
    PRIMARY KEY (id)
);

CREATE INDEX main3 ON staging.customer_research (category_id);