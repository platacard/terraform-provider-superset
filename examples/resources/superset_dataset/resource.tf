resource "superset_dataset" "example" {
  table_name    = "example_table"
  database_name = "PostgreSQL"
  schema        = "public"
  sql           = <<-EOF
    WITH RECURSIVE recursive_sequence AS (
      SELECT 1 as level, 1 as value, 'Level One' as description
      UNION ALL
      SELECT
        level + 1,
        value * 2,
        CASE
          WHEN level + 1 <= 3 THEN CONCAT('Level ',
            CASE level + 1
              WHEN 2 THEN 'Two'
              WHEN 3 THEN 'Three'
            END)
          ELSE 'Max Level'
        END
      FROM recursive_sequence
      WHERE level < 5
    ),
    calculated_data AS (
      SELECT
        level,
        value,
        description,
        value * 1.5 as weighted_value,
        ROUND(LN(value::numeric), 2) as log_value,  -- Изменено LN + приведение к numeric
        ROW_NUMBER() OVER (ORDER BY level) as row_num
      FROM recursive_sequence
    )
    SELECT
      level as hierarchy_level,
      value as base_value,
      weighted_value,
      log_value,
      description as level_description,
      CASE
        WHEN row_num % 2 = 0 THEN 'Even'
        ELSE 'Odd'
      END as parity,
      NOW() as generated_timestamp
    FROM calculated_data
    ORDER BY level
  EOF
}
