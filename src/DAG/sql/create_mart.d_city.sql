DROP TABLE IF EXISTS mart.d_city CASCADE;

CREATE TABLE mart.d_city (
	id serial4 NOT NULL,
	city_id int4 NULL,
	city_name varchar(50) NULL,
	CONSTRAINT d_city_city_id_key UNIQUE (city_id),
	CONSTRAINT d_city_pkey PRIMARY KEY (id)
);
CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);