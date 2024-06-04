-- :dbt run --models generate_test_data

WITH base_data AS (
  SELECT
    '{{ dbt_utils.surrogate_key(["id"]) }}' AS id,
    '{{ dbt_utils.random_int(100, 999) }}' AS random_number,
    '{{ dbt_utils.random_date("2022-01-01", "2022-12-31") }}' AS random_date,
    '{{ dbt_utils.random_choice(["A", "B", "C"]) }}' AS category
  FROM
    {{ dbt_utils.generate_series(1, 1000) }} -- 生成1000条记录
)

SELECT
  id,
  random_number,
  random_date,
  category
FROM
  base_data