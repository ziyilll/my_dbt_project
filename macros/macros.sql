{% macro calculate_trace_ontime_node_cnt(ts_pairs, condition) %}
    {% for pair in ts_pairs %}
        {% if condition %}
            if(({{ pair[0] }} - {{ pair[1] }}) / 3600 <= 2 AND send_time - {{ pair[1] }} < 0, 1, 0) +
        {% else %}
            if(({{ pair[0] }} - {{ pair[1] }}) / 3600 <= 2, 1, 0) +
        {% endif %}
    {% endfor %}
    0
{% endmacro %}

{% macro calculate_trace_ontime_node_cnt_fail(ts_pairs, condition) %}
    {% for pair in ts_pairs %}
        {% if condition %}
            if(({{ pair[0] }} - {{ pair[1] }}) / 3600 > 2 AND send_time - {{ pair[1] }} < 0, 1, 0) +
        {% else %}
            if(({{ pair[0] }} - {{ pair[1] }}) / 3600 > 2, 1, 0) +
        {% endif %}
    {% endfor %}
    0
{% endmacro %}


{% macro calculate_trace_ontime_node_cnt_fail_seller(ts_pairs) %}
    {% for pair in ts_pairs %}
        if((send_time - {{ pair[1] }}) / 3600 > 2, 1, 0) +
    {% endfor %}
    0
{% endmacro %}


{% macro calculate_trace_back_node_cnt(ts_pairs, condition) %}
    {% for pair in ts_pairs %}
        {% if condition %}
            if(({{ pair[1] }} > 0, 1, 0) AND send_time - {{ pair[1] }} < 0, 1, 0) +
        {% else %}
            if({{ pair[1] }} > 0, 1, 0) +
        {% endif %}
    {% endfor %}
    0
{% endmacro %}


{% macro calculate_tx_field(condition, true_value, false_value, field_name) %}
    IF(
        {{ condition }},
        {{ true_value }},
        {{ false_value }}
    ) AS {{ field_name }}
{% endmacro %}

{% macro calculate_survey_fields(ticket_channel, rate, im_survey_rate, create_im_ticket_type, is_pending72h_2_close, survey_submit_date_field, survey_submit_volume_field, survey_satisfied_volume_field) %}
    {{ survey_submit_date_field }} AS survey_submit_date,
    {{ survey_submit_volume_field }} AS survey_submit_volume,
    {{ survey_satisfied_volume_field }} AS survey_satisfied_volume
{% endmacro %}

{% macro survey_submit_date_field(ticket_channel, rate, im_survey_rate, create_im_ticket_type, is_pending72h_2_close) %}
    CASE
        WHEN {{ ticket_channel }} != 3 AND {{ rate }} BETWEEN 1 AND 5 THEN FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(survey_submit_date AS STRING),'yyyyMMdd'),'yyyy-MM-dd')
        WHEN {{ ticket_channel }} = 3 AND {{ im_survey_rate }} IS NOT NULL AND NVL({{ create_im_ticket_type }},-1) = 1 THEN FROM_UNIXTIME(im_survey_submit_ts, 'yyyy-MM-dd')
        WHEN {{ ticket_channel }} = 3 AND NVL({{ create_im_ticket_type }},-1) != 1 AND {{ rate }} IS NOT NULL THEN FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(survey_submit_date AS STRING),'yyyyMMdd'),'yyyy-MM-dd')
        WHEN {{ ticket_channel }} = 3 AND NVL({{ create_im_ticket_type }},-1) != 1 AND {{ is_pending72h_2_close }} != 1 AND {{ rate }} IS NULL AND {{ im_survey_rate }} IS NOT NULL THEN FROM_UNIXTIME(im_survey_submit_ts, 'yyyy-MM-dd')
        WHEN NVL({{ create_im_ticket_type }},-1) != 1 AND {{ is_pending72h_2_close }} = 1 THEN NULL ELSE NULL
    END
{% endmacro %}

{% macro survey_submit_volume_field(ticket_channel, rate, im_survey_rate, create_im_ticket_type, is_pending72h_2_close) %}
    CASE
        WHEN {{ ticket_channel }} != 3 AND {{ rate }} BETWEEN 1 AND 5 THEN 1
        WHEN {{ ticket_channel }} = 3 AND {{ im_survey_rate }} IS NOT NULL AND NVL({{ create_im_ticket_type }},-1) = 1 THEN 1
        WHEN {{ ticket_channel }} = 3 AND NVL({{ create_im_ticket_type }},-1) != 1 AND {{ rate }} IS NOT NULL THEN 1
        WHEN {{ ticket_channel }} = 3 AND NVL({{ create_im_ticket_type }},-1) != 1 AND {{ is_pending72h_2_close }} != 1 AND {{ rate }} IS NULL AND {{ im_survey_rate }} IS NOT NULL THEN 1
        WHEN NVL({{ create_im_ticket_type }},-1) != 1 AND {{ is_pending72h_2_close }} = 1 THEN 0
        ELSE 0
    END
{% endmacro %}

{% macro survey_satisfied_volume_field(ticket_channel, rate, im_survey_rate, create_im_ticket_type, is_pending72h_2_close) %}
    CASE
        WHEN {{ ticket_channel }} != 3 AND {{ rate }} BETWEEN 4 AND 5 THEN 1
        WHEN {{ ticket_channel }} = 3 AND {{ im_survey_rate }} BETWEEN 4 AND 5 AND NVL({{ create_im_ticket_type }},-1) = 1 THEN 1
        WHEN {{ ticket_channel }} = 3 AND NVL({{ create_im_ticket_type }},-1) != 1 AND {{ rate }} BETWEEN 4 AND 5 THEN 1
        WHEN {{ ticket_channel }} = 3 AND NVL({{ create_im_ticket_type }},-1) != 1 AND {{ is_pending72h_2_close }} != 1 AND {{ rate }} IS NULL AND {{ im_survey_rate }} BETWEEN 4 AND 5 THEN 1
        WHEN NVL({{ create_im_ticket_type }},-1) != 1 AND {{ is_pending72h_2_close }} = 1 THEN 0
        ELSE 0
    END
{% endmacro %}