truncate table mart.f_customer_retention;

insert into mart.f_customer_retention (
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_name,
	period_id,
	category_id,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded
)

with by_customer_id as (
select
	dc.week_of_year period_id,
	customer_id,
	count(customer_id) orders_count,
	count(customer_id) filter (where payment_amount < 0) refunded_customer,
	sum(payment_amount) filter (where payment_amount > 0) revenue
from
	mart.f_sales fs2
join mart.d_calendar dc on dc.date_id = fs2.date_id
group by
	dc.week_of_year,
	customer_id
)

select

	 count(customer_id) filter (where orders_count = 1) new_customers_count,
     count(customer_id) filter (where orders_count > 1) returning_customers_count,
     count(customer_id) filter (where refunded_customer > 0) refunded_customer_count,
	'weekly' period_name,
	 period_id,
	 null category_id,
	 sum(revenue) filter (where orders_count = 1) new_customers_revenue,
	 sum(revenue) filter (where orders_count > 1) returning_customers_revenue,
	 sum(refunded_customer) customers_refunded

from by_customer_id
group by period_name, period_id