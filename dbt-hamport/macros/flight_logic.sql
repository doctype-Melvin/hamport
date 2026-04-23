{% macro get_flight_status(actual_time, planned_time, cancelled) %}
    case
        when {{ cancelled }} is true then 'Cancelled'
        when {{ actual_time }} is null and (now() - {{ planned_time }}) > interval '12 hours' then 'Data Stale'
        when {{ actual_time }} is null and {{ planned_time }} > now() then 'Planned'
        when {{ actual_time }} is null and {{ planned_time }} <= now() then 'En Route / Delayed'
        else 'Completed'
    end
{% endmacro %}

{% macro get_delay_minutes(actual_time, planned_time, cancelled)%}
     case
        when {{ cancelled }} is true then null
        when {{ actual_time }} is null and {{ planned_time }} > now() then 0 -- future scheduled flight
        when {{ actual_time }} is null and {{ planned_time }} <= now() then -- delayed flight not landed yet
            extract(epoch from (now() - {{ planned_time }})) / 60
        else extract(epoch from ({{ actual_time }} - {{ planned_time }})) / 60 -- flight arrived
    end
{% endmacro %}