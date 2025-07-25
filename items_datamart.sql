DROP EXTERNAL TABLE IF EXISTS "na-tarasova"."seller_items" CASCADE;

CREATE EXTERNAL TABLE "na-tarasova"."seller_items"(
    sku_id BIGINT,
    title TEXT,
    category TEXT,
    brand TEXT,
    seller TEXT,
    group_type TEXT,
    country TEXT,
    availability_items_count BIGINT,
    ordered_items_count BIGINT,
    warehouses_count BIGINT,
    item_price BIGINT,
    goods_sold_count BIGINT,
    item_rate FLOAT8,
    days_on_sell BIGINT,
    avg_percent_to_sold BIGINT,
    returned_items_count INTEGER,
    potential_revenue BIGINT,
    total_revenue BIGINT,
    avg_daily_sales FLOAT8,
    days_to_sold FLOAT8,
    item_rate_percent FLOAT8

    ) LOCATION ('pxf://startde-project/na-tarasova/seller_items?PROFILE=s3:parquet&accesskey=r7LX3wSCP5ZK1yXupKEVVG&secretkey=3UnRR8kC8Tvq7vNXibyjW5XxS38dUwvojkKzZWP5p6Uw&endpoint=hb.vkcloud-storage.ru')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
