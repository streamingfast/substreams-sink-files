mod abi;
mod pb;
use num_traits::cast::ToPrimitive;
use pb::contract::v1 as contract;
use pb::sf::substreams::ethereum::v1::Events;
use pb::sf::substreams::v1::Clock;
use substreams_ethereum::Event;

substreams_ethereum::init!();

fn map_eth_usdc_pool_events(clock: &Clock, input: &Events, events: &mut contract::Events) {
    events.swaps.extend(input.events.iter().filter_map(|evt| {
        let log = evt.log.as_ref().unwrap();

        if let Some(event) = abi::eth_usdc_pool_contract::events::Swap::match_and_decode(log) {
            return Some(contract::Swap {
                evt_tx_hash: evt.tx_hash.clone(),
                evt_index: log.block_index,
                evt_block_time: clock.timestamp.clone(),
                evt_block_number: clock.number,
                amount0: event.amount0.to_string(),
                amount1: event.amount1.to_string(),
                liquidity: event.liquidity.to_string(),
                recipient: event.recipient,
                sender: event.sender,
                sqrt_price_x96: event.sqrt_price_x96.to_string(),
                tick: Into::<num_bigint::BigInt>::into(event.tick)
                    .to_i64()
                    .unwrap(),
            });
        }

        None
    }));
}

#[substreams::handlers::map]
fn map_events(clock: Clock, input: Events) -> Result<contract::Events, substreams::errors::Error> {
    let mut events = contract::Events::default();
    map_eth_usdc_pool_events(&clock, &input, &mut events);
    Ok(events)
}
