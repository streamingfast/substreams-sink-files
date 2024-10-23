mod abi;
mod pb;
use num_traits::cast::ToPrimitive;
use pb::contract::v1 as contract;
use pb::sf::substreams::ethereum::v1::Events;
use pb::sf::substreams::v1::Clock;
use substreams_ethereum::Event;

substreams_ethereum::init!();

fn map_eth_usdc_pool_events(clock: &Clock, input: &Events, events: &mut contract::Events) {
    events
        .eth_usdc_pool_swaps
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();

            if let Some(event) = abi::eth_usdc_pool_contract::events::Swap::match_and_decode(log) {
                return Some(contract::EthUsdcPoolSwap {
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

    events
        .eth_usdc_pool_burns
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();

            if let Some(event) = abi::eth_usdc_pool_contract::events::Burn::match_and_decode(log) {
                return Some(contract::EthUsdcPoolBurn {
                    evt_tx_hash: evt.tx_hash.clone(),
                    evt_index: log.block_index,
                    evt_block_time: clock.timestamp.clone(),
                    evt_block_number: clock.number,
                    amount: event.amount.to_string(),
                    amount0: event.amount0.to_string(),
                    amount1: event.amount1.to_string(),
                    owner: event.owner,
                    tick_lower: Into::<num_bigint::BigInt>::into(event.tick_lower)
                        .to_i64()
                        .unwrap(),
                    tick_upper: Into::<num_bigint::BigInt>::into(event.tick_upper)
                        .to_i64()
                        .unwrap(),
                });
            }

            None
        }));
    events
        .eth_usdc_pool_collects
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();
            if let Some(event) = abi::eth_usdc_pool_contract::events::Collect::match_and_decode(log)
            {
                return Some(contract::EthUsdcPoolCollect {
                    evt_tx_hash: evt.tx_hash.clone(),
                    evt_index: log.block_index,
                    evt_block_time: clock.timestamp.clone(),
                    evt_block_number: clock.number,
                    amount0: event.amount0.to_string(),
                    amount1: event.amount1.to_string(),
                    owner: event.owner,
                    recipient: event.recipient,
                    tick_lower: Into::<num_bigint::BigInt>::into(event.tick_lower)
                        .to_i64()
                        .unwrap(),
                    tick_upper: Into::<num_bigint::BigInt>::into(event.tick_upper)
                        .to_i64()
                        .unwrap(),
                });
            }

            None
        }));
    events
        .eth_usdc_pool_collect_protocols
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();
            if let Some(event) =
                abi::eth_usdc_pool_contract::events::CollectProtocol::match_and_decode(log)
            {
                return Some(contract::EthUsdcPoolCollectProtocol {
                    evt_tx_hash: evt.tx_hash.clone(),
                    evt_index: log.block_index,
                    evt_block_time: clock.timestamp.clone(),
                    evt_block_number: clock.number,
                    amount0: event.amount0.to_string(),
                    amount1: event.amount1.to_string(),
                    recipient: event.recipient,
                    sender: event.sender,
                });
            }

            None
        }));
    events
        .eth_usdc_pool_flashes
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();
            if let Some(event) = abi::eth_usdc_pool_contract::events::Flash::match_and_decode(log) {
                return Some(contract::EthUsdcPoolFlash {
                    evt_tx_hash: evt.tx_hash.clone(),
                    evt_index: log.block_index,
                    evt_block_time: clock.timestamp.clone(),
                    evt_block_number: clock.number,
                    amount0: event.amount0.to_string(),
                    amount1: event.amount1.to_string(),
                    paid0: event.paid0.to_string(),
                    paid1: event.paid1.to_string(),
                    recipient: event.recipient,
                    sender: event.sender,
                });
            }

            None
        }));

    events.eth_usdc_pool_increase_observation_cardinality_nexts.extend(input.events.iter().filter_map(|evt| {
    let log = evt.log.as_ref().unwrap();
        if let Some(event) = abi::eth_usdc_pool_contract::events::IncreaseObservationCardinalityNext::match_and_decode(log) {
                        return Some(contract::EthUsdcPoolIncreaseObservationCardinalityNext {
                            evt_tx_hash: evt.tx_hash.clone(),
                            evt_index: log.block_index,
                            evt_block_time: clock.timestamp.clone(),
                            evt_block_number: clock.number,
                            observation_cardinality_next_new: event.observation_cardinality_next_new.to_u64(),
                            observation_cardinality_next_old: event.observation_cardinality_next_old.to_u64(),
                        });
                    }

                    None
                }));
    events
        .eth_usdc_pool_initializes
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();
            if let Some(event) =
                abi::eth_usdc_pool_contract::events::Initialize::match_and_decode(log)
            {
                return Some(contract::EthUsdcPoolInitialize {
                    evt_tx_hash: evt.tx_hash.clone(),
                    evt_index: log.block_index,
                    evt_block_time: clock.timestamp.clone(),
                    evt_block_number: clock.number,
                    sqrt_price_x96: event.sqrt_price_x96.to_string(),
                    tick: Into::<num_bigint::BigInt>::into(event.tick)
                        .to_i64()
                        .unwrap(),
                });
            }

            None
        }));
    events
        .eth_usdc_pool_mints
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();
            if let Some(event) = abi::eth_usdc_pool_contract::events::Mint::match_and_decode(log) {
                return Some(contract::EthUsdcPoolMint {
                    evt_tx_hash: evt.tx_hash.clone(),
                    evt_index: log.block_index,
                    evt_block_time: clock.timestamp.clone(),
                    evt_block_number: clock.number,
                    amount: event.amount.to_string(),
                    amount0: event.amount0.to_string(),
                    amount1: event.amount1.to_string(),
                    owner: event.owner,
                    sender: event.sender,
                    tick_lower: Into::<num_bigint::BigInt>::into(event.tick_lower)
                        .to_i64()
                        .unwrap(),
                    tick_upper: Into::<num_bigint::BigInt>::into(event.tick_upper)
                        .to_i64()
                        .unwrap(),
                });
            }

            None
        }));
    events
        .eth_usdc_pool_set_fee_protocols
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();
            if let Some(event) =
                abi::eth_usdc_pool_contract::events::SetFeeProtocol::match_and_decode(log)
            {
                return Some(contract::EthUsdcPoolSetFeeProtocol {
                    evt_tx_hash: evt.tx_hash.clone(),
                    evt_index: log.block_index,
                    evt_block_time: clock.timestamp.clone(),
                    evt_block_number: clock.number,
                    fee_protocol0_new: event.fee_protocol0_new.to_u64(),
                    fee_protocol0_old: event.fee_protocol0_old.to_u64(),
                    fee_protocol1_new: event.fee_protocol1_new.to_u64(),
                    fee_protocol1_old: event.fee_protocol1_old.to_u64(),
                });
            }

            None
        }));
    events
        .eth_usdc_pool_swaps
        .extend(input.events.iter().filter_map(|evt| {
            let log = evt.log.as_ref().unwrap();
            if let Some(event) = abi::eth_usdc_pool_contract::events::Swap::match_and_decode(log) {
                return Some(contract::EthUsdcPoolSwap {
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
