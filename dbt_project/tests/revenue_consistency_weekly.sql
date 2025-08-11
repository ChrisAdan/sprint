-- tests/revenue_consistency_weekly.sql
with mart as (
    select country, week_start, sum(weekly_revenue) as mart_revenue
    from {{ ref('country_weekly_revenue') }}
    group by country, week_start
),
raw as (
    select country, date_trunc('week', event_datetime::timestamp) as week_start, sum(purchase_price) as raw_revenue
    from {{ 'sprint_raw', 'event_transaction' }}
    group by country, week_start
)
select mart.country, mart.week_start, mart_revenue, raw_revenue
from mart
join raw using (country, week_start)
where abs(mart_revenue - raw_revenue) > 0.01
