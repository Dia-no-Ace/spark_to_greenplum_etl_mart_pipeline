CREATE OR REPLACE VIEW "na-tarasova"."item_brands_view" as
select brand, 
       group_type, 
       country, 
       sum(potential_revenue) as potential_revenue,
       sum(total_revenue) as total_revenue,
       count(sku_id) as items_count
from "na-tarasova"."seller_items"
group by 1, 2, 3;