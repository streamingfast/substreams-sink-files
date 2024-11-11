with counts as (
select
    (select COUNT(*) from file('$PARQUET_TABLE/burns/*', Parquet)) as usdc_burns_count,
    (select COUNT(*) from file('$PARQUET_TABLE/mints/*', Parquet)) as usdc_mints_count
) select * from counts
