CREATE OR REPLACE VIEW "na-tarasova"."unreliable_sellers_view" as
with 
cte_1 as ( select seller,
		   availability_items_count,	
	       case when availability_items_count > ordered_items_count then 1 else 0 end as avail_more_then_ordered,
	       case when days_on_sell > 100 then 1 else 0 end as days_more_100
	FROM "na-tarasova"."seller_items"
	),
cte_2 as (
	select seller, 
		   sum(availability_items_count) as total_overload_items_count,
	       sum(avail_more_then_ordered) as total_avail_more_then_ordered,
	       sum(days_more_100) as total_days_more_100
	from cte_1
	group by seller
)
select seller,
		total_overload_items_count,
	   total_avail_more_then_ordered > 0 and total_days_more_100 > 0 as is_unreliable
from cte_2;