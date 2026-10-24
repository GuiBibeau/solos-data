//! Anchor discriminators of the instructions and events the spot decoders read:
//! `sha256("global:<instruction>")[..8]` and `sha256("event:<Event>")[..8]`, checked against
//! [`crate::venues::idl::discriminator`] by the test below.

/// `global:swap`.
pub const IX_SWAP: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];
/// `global:swap_v2`.
pub const IX_SWAP_V2: [u8; 8] = [0x2b, 0x04, 0xed, 0x0b, 0x1a, 0xc9, 0x1e, 0x62];
/// `global:swap_router_base_in`.
pub const IX_SWAP_ROUTER_BASE_IN: [u8; 8] = [0x45, 0x7d, 0x73, 0xda, 0xf5, 0xba, 0xf2, 0xc4];
/// `global:swap_base_input`.
pub const IX_SWAP_BASE_INPUT: [u8; 8] = [0x8f, 0xbe, 0x5a, 0xda, 0xc4, 0x1e, 0x33, 0xde];
/// `global:swap_base_output`.
pub const IX_SWAP_BASE_OUTPUT: [u8; 8] = [0x37, 0xd9, 0x62, 0x56, 0xa3, 0x4a, 0xb4, 0xad];
/// `global:increase_liquidity`.
pub const IX_INCREASE_LIQUIDITY: [u8; 8] = [0x2e, 0x9c, 0xf3, 0x76, 0x0d, 0xcd, 0xfb, 0xb2];
/// `global:increase_liquidity_v2`.
pub const IX_INCREASE_LIQUIDITY_V2: [u8; 8] = [0x85, 0x1d, 0x59, 0xdf, 0x45, 0xee, 0xb0, 0x0a];
/// `global:decrease_liquidity`.
pub const IX_DECREASE_LIQUIDITY: [u8; 8] = [0xa0, 0x26, 0xd0, 0x6f, 0x68, 0x5b, 0x2c, 0x01];
/// `global:decrease_liquidity_v2`.
pub const IX_DECREASE_LIQUIDITY_V2: [u8; 8] = [0x3a, 0x7f, 0xbc, 0x3e, 0x4f, 0x52, 0xc4, 0x60];
/// `global:initialize`.
pub const IX_INITIALIZE: [u8; 8] = [0xaf, 0xaf, 0x6d, 0x1f, 0x0d, 0x98, 0x9b, 0xed];
/// `global:deposit`.
pub const IX_DEPOSIT: [u8; 8] = [0xf2, 0x23, 0xc6, 0x89, 0x52, 0xe1, 0xf2, 0xb6];
/// `global:withdraw`.
pub const IX_WITHDRAW: [u8; 8] = [0xb7, 0x12, 0x46, 0x9c, 0x94, 0x6d, 0xa1, 0x22];
/// `global:buy_exact_in`.
pub const IX_BUY_EXACT_IN: [u8; 8] = [0xfa, 0xea, 0x0d, 0x7b, 0xd5, 0x9c, 0x13, 0xec];
/// `global:buy_exact_out`.
pub const IX_BUY_EXACT_OUT: [u8; 8] = [0x18, 0xd3, 0x74, 0x28, 0x69, 0x03, 0x99, 0x38];
/// `global:sell_exact_in`.
pub const IX_SELL_EXACT_IN: [u8; 8] = [0x95, 0x27, 0xde, 0x9b, 0xd3, 0x7c, 0x98, 0x1a];
/// `global:sell_exact_out`.
pub const IX_SELL_EXACT_OUT: [u8; 8] = [0x5f, 0xc8, 0x47, 0x22, 0x08, 0x09, 0x0b, 0xa6];
/// `global:two_hop_swap`.
pub const IX_TWO_HOP_SWAP: [u8; 8] = [0xc3, 0x60, 0xed, 0x6c, 0x44, 0xa2, 0xdb, 0xe6];
/// `global:two_hop_swap_v2`.
pub const IX_TWO_HOP_SWAP_V2: [u8; 8] = [0xba, 0x8f, 0xd1, 0x1d, 0xfe, 0x02, 0xc2, 0x75];
/// `global:swap2`.
pub const IX_SWAP2: [u8; 8] = [0x41, 0x4b, 0x3f, 0x4c, 0xeb, 0x5b, 0x5b, 0x88];
/// `global:swap_exact_out`.
pub const IX_SWAP_EXACT_OUT: [u8; 8] = [0xfa, 0x49, 0x65, 0x21, 0x26, 0xcf, 0x4b, 0xb8];
/// `global:swap_exact_out2`.
pub const IX_SWAP_EXACT_OUT2: [u8; 8] = [0x2b, 0xd7, 0xf7, 0x84, 0x89, 0x3c, 0xf3, 0x51];
/// `global:swap_with_price_impact`.
pub const IX_SWAP_WITH_PRICE_IMPACT: [u8; 8] = [0x38, 0xad, 0xe6, 0xd0, 0xad, 0xe4, 0x9c, 0xcd];
/// `global:swap_with_price_impact2`.
pub const IX_SWAP_WITH_PRICE_IMPACT2: [u8; 8] = [0x4a, 0x62, 0xc0, 0xd6, 0xb1, 0x33, 0x4b, 0x33];
/// `global:add_balance_liquidity`.
pub const IX_ADD_BALANCE_LIQUIDITY: [u8; 8] = [0xa8, 0xe3, 0x32, 0x3e, 0xbd, 0xab, 0x54, 0xb0];
/// `global:add_imbalance_liquidity`.
pub const IX_ADD_IMBALANCE_LIQUIDITY: [u8; 8] = [0x4f, 0x23, 0x7a, 0x54, 0xad, 0x0f, 0x5d, 0xbf];
/// `global:bootstrap_liquidity`.
pub const IX_BOOTSTRAP_LIQUIDITY: [u8; 8] = [0x04, 0xe4, 0xd7, 0x47, 0xe1, 0xfd, 0x77, 0xce];
/// `global:remove_balance_liquidity`.
pub const IX_REMOVE_BALANCE_LIQUIDITY: [u8; 8] = [0x85, 0x6d, 0x2c, 0xb3, 0x38, 0xee, 0x72, 0x21];
/// `global:remove_liquidity_single_side`.
pub const IX_REMOVE_LIQUIDITY_SINGLE_SIDE: [u8; 8] =
    [0x54, 0x54, 0xb1, 0x42, 0xfe, 0xb9, 0x0a, 0xfb];
/// `global:swap2_with_transfer_hook`.
pub const IX_SWAP2_WITH_TRANSFER_HOOK: [u8; 8] = [0xb7, 0x5d, 0x99, 0x28, 0x18, 0xe6, 0xc2, 0x97];
/// `global:buy`.
pub const IX_BUY: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
/// `global:sell`.
pub const IX_SELL: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];
/// `global:buy_exact_quote_in`.
pub const IX_BUY_EXACT_QUOTE_IN: [u8; 8] = [0xc6, 0x2e, 0x15, 0x52, 0xb4, 0xd9, 0xe8, 0x70];
/// `global:buy_v2`.
pub const IX_BUY_V2: [u8; 8] = [0xb8, 0x17, 0xee, 0x61, 0x67, 0xc5, 0xd3, 0x3d];
/// `global:sell_v2`.
pub const IX_SELL_V2: [u8; 8] = [0x5d, 0xf6, 0x82, 0x3c, 0xe7, 0xe9, 0x40, 0xb2];
/// `global:buy_exact_quote_in_v2`.
pub const IX_BUY_EXACT_QUOTE_IN_V2: [u8; 8] = [0xc2, 0xab, 0x1c, 0x46, 0x68, 0x4d, 0x5b, 0x2f];
/// `global:multi_hop_swap`.
pub const IX_MULTI_HOP_SWAP: [u8; 8] = [0x2b, 0x64, 0x49, 0x13, 0xe9, 0xf6, 0x6f, 0x94];
/// `global:buy_exact_sol_in`.
pub const IX_BUY_EXACT_SOL_IN: [u8; 8] = [0x38, 0xfc, 0x74, 0x08, 0x9e, 0xdf, 0xcd, 0x5f];
/// `global:buy_v3`.
pub const IX_BUY_V3: [u8; 8] = [0x07, 0x05, 0x1d, 0xc4, 0xf5, 0x17, 0x65, 0x50];
/// `global:sell_v3`.
pub const IX_SELL_V3: [u8; 8] = [0x1c, 0x92, 0xde, 0x77, 0x26, 0xc4, 0x69, 0xd5];
/// `global:buy_exact_quote_in_v3`.
pub const IX_BUY_EXACT_QUOTE_IN_V3: [u8; 8] = [0xe1, 0xf7, 0x50, 0x1e, 0xd5, 0xb3, 0x84, 0x88];
/// `global:multi_hop_curve_swap`.
pub const IX_MULTI_HOP_CURVE_SWAP: [u8; 8] = [0xe1, 0x9a, 0x75, 0x16, 0xd7, 0x56, 0xf6, 0x67];
/// `global:boost_buy_and_burn`.
pub const IX_BOOST_BUY_AND_BURN: [u8; 8] = [0x69, 0x44, 0x06, 0xaf, 0x00, 0x07, 0x23, 0xa2];
/// `event:SwapEvent`.
pub const EV_SWAP_EVENT: [u8; 8] = [0x40, 0xc6, 0xcd, 0xe8, 0x26, 0x08, 0x71, 0xe2];
/// `event:LpChangeEvent`.
pub const EV_LP_CHANGE_EVENT: [u8; 8] = [0x79, 0xa3, 0xcd, 0xc9, 0x39, 0xda, 0x75, 0x3c];
/// `event:IncreaseLiquidityEvent`.
pub const EV_INCREASE_LIQUIDITY_EVENT: [u8; 8] = [0x31, 0x4f, 0x69, 0xd4, 0x20, 0x22, 0x1e, 0x54];
/// `event:DecreaseLiquidityEvent`.
pub const EV_DECREASE_LIQUIDITY_EVENT: [u8; 8] = [0x3a, 0xde, 0x56, 0x3a, 0x44, 0x32, 0x55, 0x38];
/// `event:CreatePersonalPositionEvent`.
pub const EV_CREATE_PERSONAL_POSITION_EVENT: [u8; 8] =
    [0x64, 0x1e, 0x57, 0xf9, 0xc4, 0xdf, 0x9a, 0xce];
/// `event:PoolCreatedEvent`.
pub const EV_POOL_CREATED_EVENT: [u8; 8] = [0x19, 0x5e, 0x4b, 0x2f, 0x70, 0x63, 0x35, 0x3f];
/// `event:TradeEvent`.
pub const EV_TRADE_EVENT: [u8; 8] = [0xbd, 0xdb, 0x7f, 0xd3, 0x4e, 0xe6, 0x61, 0xee];
/// `event:PoolCreateEvent`.
pub const EV_POOL_CREATE_EVENT: [u8; 8] = [0x97, 0xd7, 0xe2, 0x09, 0x76, 0xa1, 0x73, 0xae];
/// `event:Traded`.
pub const EV_TRADED: [u8; 8] = [0xe1, 0xca, 0x49, 0xaf, 0x93, 0x2b, 0xa0, 0x96];
/// `event:LiquidityIncreased`.
pub const EV_LIQUIDITY_INCREASED: [u8; 8] = [0x1e, 0x07, 0x90, 0xb5, 0x66, 0xfe, 0x9b, 0xa1];
/// `event:LiquidityDecreased`.
pub const EV_LIQUIDITY_DECREASED: [u8; 8] = [0xa6, 0x01, 0x24, 0x47, 0x70, 0xca, 0xb5, 0xab];
/// `event:PoolInitialized`.
pub const EV_POOL_INITIALIZED: [u8; 8] = [0x64, 0x76, 0xad, 0x57, 0x0c, 0xc6, 0xfe, 0xe5];
/// `event:Swap`.
pub const EV_SWAP: [u8; 8] = [0x51, 0x6c, 0xe3, 0xbe, 0xcd, 0xd0, 0x0a, 0xc4];
/// `event:Swap2Evt`.
pub const EV_SWAP2_EVT: [u8; 8] = [0x2e, 0x74, 0x52, 0xd7, 0x94, 0x1b, 0x54, 0x4d];
/// `event:AddLiquidity`.
pub const EV_ADD_LIQUIDITY: [u8; 8] = [0x1f, 0x5e, 0x7d, 0x5a, 0xe3, 0x34, 0x3d, 0xba];
/// `event:RemoveLiquidity`.
pub const EV_REMOVE_LIQUIDITY: [u8; 8] = [0x74, 0xf4, 0x61, 0xe8, 0x67, 0x1f, 0x98, 0x3a];
/// `event:LbPairCreate`.
pub const EV_LB_PAIR_CREATE: [u8; 8] = [0xb9, 0x4a, 0xfc, 0x7d, 0x1b, 0xd7, 0xbc, 0x6f];
/// `event:PoolCreated`.
pub const EV_POOL_CREATED: [u8; 8] = [0xca, 0x2c, 0x29, 0x58, 0x68, 0xdc, 0x9d, 0x52];
/// `event:EvtSwap`.
pub const EV_EVT_SWAP: [u8; 8] = [0x1b, 0x3c, 0x15, 0xd5, 0x8a, 0xaa, 0xbb, 0x93];
/// `event:EvtSwap2`.
pub const EV_EVT_SWAP2: [u8; 8] = [0xbd, 0x42, 0x33, 0xa8, 0x26, 0x50, 0x75, 0x99];
/// `event:EvtLiquidityChange`.
pub const EV_EVT_LIQUIDITY_CHANGE: [u8; 8] = [0xc5, 0xab, 0x4e, 0x7f, 0xe0, 0xd3, 0x57, 0x0d];
/// `event:EvtAddLiquidity`.
pub const EV_EVT_ADD_LIQUIDITY: [u8; 8] = [0xaf, 0xf2, 0x08, 0x9d, 0x1e, 0xf7, 0xb9, 0xa9];
/// `event:EvtRemoveLiquidity`.
pub const EV_EVT_REMOVE_LIQUIDITY: [u8; 8] = [0x57, 0x2e, 0x58, 0x62, 0xaf, 0x60, 0x22, 0x5b];
/// `event:EvtInitializePool`.
pub const EV_EVT_INITIALIZE_POOL: [u8; 8] = [0xe4, 0x32, 0xf6, 0x55, 0xcb, 0x42, 0x86, 0x25];
/// `event:EvtSwap2WithTransferHook`.
pub const EV_EVT_SWAP2_WITH_TRANSFER_HOOK: [u8; 8] =
    [0x86, 0x3b, 0xa8, 0x78, 0x5e, 0x33, 0x72, 0xe7];
/// `event:EvtSwap3`.
pub const EV_EVT_SWAP3: [u8; 8] = [0xbb, 0x77, 0xf8, 0x0d, 0xaf, 0xff, 0x40, 0x3f];
/// `event:EvtSwap3WithTransferHook`.
pub const EV_EVT_SWAP3_WITH_TRANSFER_HOOK: [u8; 8] =
    [0x9c, 0x58, 0xbc, 0x31, 0xe3, 0x92, 0xa5, 0x5d];
/// `event:BuyEvent`.
pub const EV_BUY_EVENT: [u8; 8] = [0x67, 0xf4, 0x52, 0x1f, 0x2c, 0xf5, 0x77, 0x77];
/// `event:SellEvent`.
pub const EV_SELL_EVENT: [u8; 8] = [0x3e, 0x2f, 0x37, 0x0a, 0xa5, 0x03, 0xdc, 0x2a];
/// `event:DepositEvent`.
pub const EV_DEPOSIT_EVENT: [u8; 8] = [0x78, 0xf8, 0x3d, 0x53, 0x1f, 0x8e, 0x6b, 0x90];
/// `event:WithdrawEvent`.
pub const EV_WITHDRAW_EVENT: [u8; 8] = [0x16, 0x09, 0x85, 0x1a, 0xa0, 0x2c, 0x47, 0xc0];
/// `event:CreatePoolEvent`.
pub const EV_CREATE_POOL_EVENT: [u8; 8] = [0xb1, 0x31, 0x0c, 0xd2, 0xa0, 0x76, 0xa7, 0x74];
/// `event:CreateEvent`.
pub const EV_CREATE_EVENT: [u8; 8] = [0x1b, 0x72, 0xa9, 0x4d, 0xde, 0xeb, 0x63, 0x76];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venues::idl::discriminator;

    #[test]
    fn every_constant_is_its_name_hashed() {
        assert_eq!(IX_SWAP, discriminator("global", "swap"));
        assert_eq!(IX_SWAP_V2, discriminator("global", "swap_v2"));
        assert_eq!(
            IX_SWAP_ROUTER_BASE_IN,
            discriminator("global", "swap_router_base_in")
        );
        assert_eq!(
            IX_SWAP_BASE_INPUT,
            discriminator("global", "swap_base_input")
        );
        assert_eq!(
            IX_SWAP_BASE_OUTPUT,
            discriminator("global", "swap_base_output")
        );
        assert_eq!(
            IX_INCREASE_LIQUIDITY,
            discriminator("global", "increase_liquidity")
        );
        assert_eq!(
            IX_INCREASE_LIQUIDITY_V2,
            discriminator("global", "increase_liquidity_v2")
        );
        assert_eq!(
            IX_DECREASE_LIQUIDITY,
            discriminator("global", "decrease_liquidity")
        );
        assert_eq!(
            IX_DECREASE_LIQUIDITY_V2,
            discriminator("global", "decrease_liquidity_v2")
        );
        assert_eq!(IX_INITIALIZE, discriminator("global", "initialize"));
        assert_eq!(IX_DEPOSIT, discriminator("global", "deposit"));
        assert_eq!(IX_WITHDRAW, discriminator("global", "withdraw"));
        assert_eq!(IX_BUY_EXACT_IN, discriminator("global", "buy_exact_in"));
        assert_eq!(IX_BUY_EXACT_OUT, discriminator("global", "buy_exact_out"));
        assert_eq!(IX_SELL_EXACT_IN, discriminator("global", "sell_exact_in"));
        assert_eq!(IX_SELL_EXACT_OUT, discriminator("global", "sell_exact_out"));
        assert_eq!(IX_TWO_HOP_SWAP, discriminator("global", "two_hop_swap"));
        assert_eq!(
            IX_TWO_HOP_SWAP_V2,
            discriminator("global", "two_hop_swap_v2")
        );
        assert_eq!(IX_SWAP2, discriminator("global", "swap2"));
        assert_eq!(IX_SWAP_EXACT_OUT, discriminator("global", "swap_exact_out"));
        assert_eq!(
            IX_SWAP_EXACT_OUT2,
            discriminator("global", "swap_exact_out2")
        );
        assert_eq!(
            IX_SWAP_WITH_PRICE_IMPACT,
            discriminator("global", "swap_with_price_impact")
        );
        assert_eq!(
            IX_SWAP_WITH_PRICE_IMPACT2,
            discriminator("global", "swap_with_price_impact2")
        );
        assert_eq!(
            IX_ADD_BALANCE_LIQUIDITY,
            discriminator("global", "add_balance_liquidity")
        );
        assert_eq!(
            IX_ADD_IMBALANCE_LIQUIDITY,
            discriminator("global", "add_imbalance_liquidity")
        );
        assert_eq!(
            IX_BOOTSTRAP_LIQUIDITY,
            discriminator("global", "bootstrap_liquidity")
        );
        assert_eq!(
            IX_REMOVE_BALANCE_LIQUIDITY,
            discriminator("global", "remove_balance_liquidity")
        );
        assert_eq!(
            IX_REMOVE_LIQUIDITY_SINGLE_SIDE,
            discriminator("global", "remove_liquidity_single_side")
        );
        assert_eq!(
            IX_SWAP2_WITH_TRANSFER_HOOK,
            discriminator("global", "swap2_with_transfer_hook")
        );
        assert_eq!(IX_BUY, discriminator("global", "buy"));
        assert_eq!(IX_SELL, discriminator("global", "sell"));
        assert_eq!(
            IX_BUY_EXACT_QUOTE_IN,
            discriminator("global", "buy_exact_quote_in")
        );
        assert_eq!(IX_BUY_V2, discriminator("global", "buy_v2"));
        assert_eq!(IX_SELL_V2, discriminator("global", "sell_v2"));
        assert_eq!(
            IX_BUY_EXACT_QUOTE_IN_V2,
            discriminator("global", "buy_exact_quote_in_v2")
        );
        assert_eq!(IX_MULTI_HOP_SWAP, discriminator("global", "multi_hop_swap"));
        assert_eq!(
            IX_BUY_EXACT_SOL_IN,
            discriminator("global", "buy_exact_sol_in")
        );
        assert_eq!(IX_BUY_V3, discriminator("global", "buy_v3"));
        assert_eq!(IX_SELL_V3, discriminator("global", "sell_v3"));
        assert_eq!(
            IX_BUY_EXACT_QUOTE_IN_V3,
            discriminator("global", "buy_exact_quote_in_v3")
        );
        assert_eq!(
            IX_MULTI_HOP_CURVE_SWAP,
            discriminator("global", "multi_hop_curve_swap")
        );
        assert_eq!(
            IX_BOOST_BUY_AND_BURN,
            discriminator("global", "boost_buy_and_burn")
        );
        assert_eq!(EV_SWAP_EVENT, discriminator("event", "SwapEvent"));
        assert_eq!(EV_LP_CHANGE_EVENT, discriminator("event", "LpChangeEvent"));
        assert_eq!(
            EV_INCREASE_LIQUIDITY_EVENT,
            discriminator("event", "IncreaseLiquidityEvent")
        );
        assert_eq!(
            EV_DECREASE_LIQUIDITY_EVENT,
            discriminator("event", "DecreaseLiquidityEvent")
        );
        assert_eq!(
            EV_CREATE_PERSONAL_POSITION_EVENT,
            discriminator("event", "CreatePersonalPositionEvent")
        );
        assert_eq!(
            EV_POOL_CREATED_EVENT,
            discriminator("event", "PoolCreatedEvent")
        );
        assert_eq!(EV_TRADE_EVENT, discriminator("event", "TradeEvent"));
        assert_eq!(
            EV_POOL_CREATE_EVENT,
            discriminator("event", "PoolCreateEvent")
        );
        assert_eq!(EV_TRADED, discriminator("event", "Traded"));
        assert_eq!(
            EV_LIQUIDITY_INCREASED,
            discriminator("event", "LiquidityIncreased")
        );
        assert_eq!(
            EV_LIQUIDITY_DECREASED,
            discriminator("event", "LiquidityDecreased")
        );
        assert_eq!(
            EV_POOL_INITIALIZED,
            discriminator("event", "PoolInitialized")
        );
        assert_eq!(EV_SWAP, discriminator("event", "Swap"));
        assert_eq!(EV_SWAP2_EVT, discriminator("event", "Swap2Evt"));
        assert_eq!(EV_ADD_LIQUIDITY, discriminator("event", "AddLiquidity"));
        assert_eq!(
            EV_REMOVE_LIQUIDITY,
            discriminator("event", "RemoveLiquidity")
        );
        assert_eq!(EV_LB_PAIR_CREATE, discriminator("event", "LbPairCreate"));
        assert_eq!(EV_POOL_CREATED, discriminator("event", "PoolCreated"));
        assert_eq!(EV_EVT_SWAP, discriminator("event", "EvtSwap"));
        assert_eq!(EV_EVT_SWAP2, discriminator("event", "EvtSwap2"));
        assert_eq!(
            EV_EVT_LIQUIDITY_CHANGE,
            discriminator("event", "EvtLiquidityChange")
        );
        assert_eq!(
            EV_EVT_ADD_LIQUIDITY,
            discriminator("event", "EvtAddLiquidity")
        );
        assert_eq!(
            EV_EVT_REMOVE_LIQUIDITY,
            discriminator("event", "EvtRemoveLiquidity")
        );
        assert_eq!(
            EV_EVT_INITIALIZE_POOL,
            discriminator("event", "EvtInitializePool")
        );
        assert_eq!(
            EV_EVT_SWAP2_WITH_TRANSFER_HOOK,
            discriminator("event", "EvtSwap2WithTransferHook")
        );
        assert_eq!(EV_EVT_SWAP3, discriminator("event", "EvtSwap3"));
        assert_eq!(
            EV_EVT_SWAP3_WITH_TRANSFER_HOOK,
            discriminator("event", "EvtSwap3WithTransferHook")
        );
        assert_eq!(EV_BUY_EVENT, discriminator("event", "BuyEvent"));
        assert_eq!(EV_SELL_EVENT, discriminator("event", "SellEvent"));
        assert_eq!(EV_DEPOSIT_EVENT, discriminator("event", "DepositEvent"));
        assert_eq!(EV_WITHDRAW_EVENT, discriminator("event", "WithdrawEvent"));
        assert_eq!(
            EV_CREATE_POOL_EVENT,
            discriminator("event", "CreatePoolEvent")
        );
        assert_eq!(EV_CREATE_EVENT, discriminator("event", "CreateEvent"));
    }
}
