select
    toDate(toStartOfMonth(evt_block_time)) as month,
    SUM(reinterpretAsUInt256(liquidity)) as total_liquidity
from
    file('$PARQUET_TABLE/swaps/*', Parquet)
group by
    toStartOfMonth(evt_block_time)
order by
    toStartOfMonth(evt_block_time) asc
;
