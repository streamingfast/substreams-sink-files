WITH burn_per_block AS (
    select
        evt_block_number as block_number,
        COUNT(evt_block_number) as burn_count
    from
        file('$PARQUET_TABLE/burns/*', Parquet)
    group by
        evt_block_number
),
mint_per_block AS (
    select
        evt_block_number as block_number,
        COUNT(evt_block_number) as mint_count
    from
        file('$PARQUET_TABLE/mints/*', Parquet)
    group by
        evt_block_number
)
select
    mint.block_number as block_number,
    mint.mint_count as mint_count,
    burn.burn_count as burn_count
from
    mint_per_block as mint
left join burn_per_block as burn
    on mint.block_number = burn.block_number
where
    mint.block_number is not null
    and burn.block_number is not null
order by
    block_number asc
;
