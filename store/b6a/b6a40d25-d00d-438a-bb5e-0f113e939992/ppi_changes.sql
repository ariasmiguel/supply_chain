ATTACH VIEW _ UUID 'c9719686-bbf4-4cfe-ba88-73e95bf98806'
(
    `date` Date,
    `metric` LowCardinality(String),
    `value` Float64,
    `is_preliminary` UInt8,
    `monthly_change` Float64,
    `yearly_change` Float64
)
AS SELECT DISTINCT
    curr.date AS date,
    curr.metric AS metric,
    curr.value AS value,
    curr.is_preliminary AS is_preliminary,
    curr.value - prev_month.value AS monthly_change,
    curr.value - prev_year.value AS yearly_change
FROM default.ppi_data AS curr
LEFT JOIN default.ppi_data AS prev_month ON (prev_month.metric = curr.metric) AND (prev_month.date = addMonths(curr.date, -1))
LEFT JOIN default.ppi_data AS prev_year ON (prev_year.metric = curr.metric) AND (prev_year.date = addYears(curr.date, -1))
