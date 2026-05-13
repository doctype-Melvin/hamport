{% macro get_flight_status(actual_time, cancelled) %}
    case
        when {{ cancelled }} is true then 'Cancelled'
        when {{ actual_time }} is null then 'Unknown'
        else 'Completed'
    end
{% endmacro %}

{% macro get_delay_minutes(actual_time, planned_time, cancelled)%}
     case
        when {{ cancelled }} is true then null
        when {{ actual_time }} is not null and {{ planned_time }} is not null 
            then extract(epoch from ({{ actual_time }} - {{ planned_time }})) / 60
        else null
    end
{% endmacro %}