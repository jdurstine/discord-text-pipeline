CREATE TABLE prod_warehouse.date_dim (
  PRIMARY KEY (date_id) NOT ENFORCED,
  date_id STRING NOT NULL,
  year INT64 NOT NULL,
  day_of_year INT64 NOT NULL,
  week_of_year INT64 NOT NULL,
  day_of_week INT64 NOT NULL,
  qtr INT64 NOT NULL,
  month INT64 NOT NULL,
  month_name STRING NOT NULL,
  day_of_month INT64 NOT NULL,
  day_name STRING NOT NULL,
  weekday_flag INT64 NOT NULL
);

INSERT INTO prod_warehouse.date_dim
SELECT
  FORMAT_DATE('%F', d) as date_id,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(DAYOFYEAR FROM d) as day_of_year,
  EXTRACT(WEEK FROM d) AS week_of_year,
  CAST(FORMAT_DATE('%w', d) AS INT64) AS day_of_week,
  CAST(FORMAT_DATE('%Q', d) AS INT64) as qtr,
  EXTRACT(MONTH FROM d) AS month,
  FORMAT_DATE('%B', d) as month_name,
  EXTRACT(DAY FROM d) AS day_of_month,
  FORMAT_DATE('%A', d) AS day_name,
  (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END) AS weekday_flag,
FROM (
  SELECT *
  FROM UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2050-01-01', INTERVAL 1 DAY)) AS d
);