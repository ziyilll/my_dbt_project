    select
        package_id,
        max(is_mall_order) as is_mall_order
    from {{ set_order_table() }}
    where date=max_pt({{ set_order_table() }})
    group by package_id