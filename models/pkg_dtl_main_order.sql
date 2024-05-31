{% set common_fields = get_common_fields() %}

select
    cast(main_order_id as bigint),
     {% for field in common_fields %}
    max({{ field }}){% if not loop.last %},{% endif %}
    {% endfor %}
from base_data a
    lateral view explode (split(main_order_id_list, ',')) as main_order_id
group by main_order_id