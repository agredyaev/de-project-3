DROP TABLE IF EXISTS mart.f_sales CASCADE;

CREATE TABLE mart.f_sales (
	id serial4 NOT NULL,
	date_id int4 NOT NULL,
	item_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	city_id int4 NOT NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	CONSTRAINT f_sales_pkey PRIMARY KEY (id)
);
CREATE INDEX f_ds1 ON mart.f_sales USING btree (date_id);
CREATE INDEX f_ds2 ON mart.f_sales USING btree (item_id);
CREATE INDEX f_ds3 ON mart.f_sales USING btree (customer_id);
CREATE INDEX f_ds4 ON mart.f_sales USING btree (city_id);


-- mart.f_sales foreign keys

ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_item_id_fkey1 FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id);