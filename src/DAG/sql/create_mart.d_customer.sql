DROP TABLE IF EXISTS mart.d_customer CASCADE;

CREATE TABLE mart.d_customer (
	id serial4 NOT NULL,
	customer_id int4 NOT NULL,
	first_name varchar(15) NULL,
	last_name varchar(15) NULL,
	city_id int4 NULL,
	CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id),
	CONSTRAINT d_customer_pkey PRIMARY KEY (id)
);
CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);