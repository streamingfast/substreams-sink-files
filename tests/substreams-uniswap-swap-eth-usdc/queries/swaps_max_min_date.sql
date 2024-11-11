select
    MIN(evt_block_time) as swap_start_date,
    MAX(evt_block_time) as swap_end_date
from
    file('$PARQUET_TABLE/swaps/*', Parquet)
;
