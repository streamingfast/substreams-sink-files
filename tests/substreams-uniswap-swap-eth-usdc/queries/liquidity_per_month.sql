select
    toDate(toStartOfMonth(evt_block_time)) as month,
    SUM(toUInt256(liquidity)) as total_liquidity
from
    file('$PARQUET_TABLE/*', Parquet)
group by
    toStartOfMonth(evt_block_time)
order by
    toStartOfMonth(evt_block_time) asc
;
