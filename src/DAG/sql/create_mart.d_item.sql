DROP TABLE IF EXISTS mart.d_item CASCADE;

CREATE TABLE mart.d_item (
    id serial4 NOT NULL,
    item_id int4 NOT NULL,
    item_name varchar(50) NULL,
    CONSTRAINT d_item_item_id_key UNIQUE (item_id),
    CONSTRAINT d_item_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);