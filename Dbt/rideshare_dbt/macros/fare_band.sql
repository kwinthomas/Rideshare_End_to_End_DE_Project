{% macro fare_band(column) %}
    case
        when {{ column }} is null      then 'Unknown'
        when {{ column }} < 15         then 'Under $15'
        when {{ column }} < 30         then '$15 to $30'
        when {{ column }} < 60         then '$30 to $60'
        when {{ column }} < 120        then '$60 to $120'
        else '$120 and over'
    end
{% endmacro %}