WITH burn_per_block AS (
    select
        evt_block_number as block_number,
        evt_tx_hash as tx_hash,
        COUNT(evt_tx_hash) as burn_count
    from
        file('$PARQUET_TABLE/burns/*', Parquet)
    group by
        (evt_block_number, evt_tx_hash)
),
mint_per_block AS (
    select
        evt_block_number as block_number,
        evt_tx_hash as tx_hash,
        COUNT(evt_tx_hash) as mint_count
    from
        file('$PARQUET_TABLE/mints/*', Parquet)
    group by
        (evt_block_number, evt_tx_hash)
)
select
    mint.block_number as block_number,
    mint.tx_hash as tx_hash,
    mint.mint_count as mint_count,
    burn.burn_count as burn_count
from
    mint_per_block as mint
left join burn_per_block as burn
    on mint.tx_hash = burn.tx_hash
where
    mint.tx_hash is not null
    and burn.tx_hash is not null
order by
    block_number asc
;
