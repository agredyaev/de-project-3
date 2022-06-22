
delete from staging.user_order_log
where date_time::date in (select date_actual from mart.d_calendar where date_actual = '{{ds}}');

delete from staging.user_activity_log
where date_time::date in (select date_actual from mart.d_calendar where date_actual = '{{ds}}');

delete from staging.customer_research
where date_id::date in (select date_actual from mart.d_calendar where date_actual = '{{ds}}');

delete from mart.f_sales
where date_id in (select date_id from mart.d_calendar where date_actual = '{{ds}}');
