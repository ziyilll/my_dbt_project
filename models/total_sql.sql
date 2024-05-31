with workday as (
    select * from {{ ref('workday') }}
),

with shop_first_order as (
    select * from {{ ref('shop_first_order') }}
),

with order_dims as (
    select * from {{ ref('order_dims') }}
),

with base_data as (
    select * from {{ ref('base_data') }}
),

with pkg_dtl_main_order as (
    select * from {{ ref('pkg_dtl_main_order')}}
),

with lgt_eng_rate_dtl as (
    select * from {{ ref('lgt_eng_rate_dtl') }}
),

with cs_ticket_dtl as (
    select * from {{ ref('cs_ticket_dtl') }}
),

with product_cl_dtl as (
    select * from {{ ref('product_cl_dtl') }}
),

with auto_cancel_pkgs as (
    select * from {{ ref('auto_cancel_pkgs') }}
),

with lgt_us_exp_detail as (
    select * from {{ ref('lgt_us_exp_detail') }}
),

with dnr_tmp as (
    select * from {{ ref('dnr_tmp') }}
),

with dnr_detail as (
    select * from {{ ref('dnr_detail') }}
)
