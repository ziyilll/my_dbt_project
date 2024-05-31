    select
        seller_id,
        min(create_date) as shop_first_order_date
    from {{ set_order_table() }}
    where date=max_pt({{ set_order_table() }})
    group by
        seller_id