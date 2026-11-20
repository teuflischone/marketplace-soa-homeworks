CREATE TABLE IF NOT EXISTS daily_metrics
(
    day Date,
    metric LowCardinality(String),
    dimension String,
    value Nullable(Float64),
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY (metric, dimension)
