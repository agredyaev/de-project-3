DROP TABLE IF EXISTS mart.f_customer_retention;

CREATE TABLE mart.f_customer_retention (
	new_customers_count int8 NOT NULL,
	returning_customers_count int8 NOT NULL,
	refunded_customer_count int8 NOT NULL,
	period_name varchar(20) NOT NULL,
	period_id int4 NOT NULL,
	category_id int8 NULL,
	new_customers_revenue numeric(10, 2) NULL,
	returning_customers_revenue numeric(10, 2) NULL,
	customers_refunded int8 NULL
);