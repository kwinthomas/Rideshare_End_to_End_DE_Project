{{ config(materialized='table') }}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date="cast('" ~ var('date_spine_end') ~ "' as date)"
    ) }}

),

enriched as (

    select
        cast(date_day as date)                          as date_key,
        date_day                                        as calendar_date,

        year(date_day)                                  as calendar_year,
        quarter(date_day)                               as calendar_quarter,
        month(date_day)                                 as calendar_month,
        day(date_day)                                   as day_of_month,
        dayofweek(date_day)                             as day_of_week_number,
        weekofyear(date_day)                            as week_of_year,

        date_format(date_day, 'MMMM')                   as month_name,
        date_format(date_day, 'MMM')                    as month_name_short,
        date_format(date_day, 'EEEE')                   as day_name,
        date_format(date_day, 'EEE')                    as day_name_short,

        concat('Q', quarter(date_day), ' ', year(date_day))  as quarter_label,
        date_format(date_day, 'MMM yyyy')                    as month_label,

        -- Sort keys. Power BI sorts text alphabetically unless given a numeric
        -- column to sort by, which is why 'April' otherwise precedes 'August'.
        year(date_day) * 100 + month(date_day)          as year_month_number,
        year(date_day) * 10 + quarter(date_day)         as year_quarter_number,

        trunc(date_day, 'MM')                           as first_day_of_month,
        last_day(date_day)                              as last_day_of_month,

        dayofweek(date_day) in (1, 7)                   as is_weekend,
        date_day = current_date()                       as is_today,
        date_day <= current_date()                      as is_past

    from spine

)

select * from enriched