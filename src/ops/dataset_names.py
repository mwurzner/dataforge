"""Descriptive public table and directory names mapped to internal collector IDs."""

PUBLIC_NAMES = {
    'e0_run_manifest': 'collection_runs',
    'e1_mempool_minutely': 'ethereum_mempool_minute_summary',
    'e1_mempool_dropped': 'ethereum_dropped_transactions',
    'e1_mempool_lifecycle': 'ethereum_transaction_lifecycle',
    'e3_mempool_divergence': 'ethereum_mempool_comparison',
    'e8_btc_mempool_lifecycle': 'bitcoin_transaction_lifecycle',
    'e8_btc_block_composition': 'bitcoin_block_composition',
    'e9_btc_mempool_divergence': 'bitcoin_mempool_comparison',
    'e10_quote_benchmark': 'ethereum_swap_quotes',
    'e11_ltc_mempool_lifecycle': 'litecoin_transaction_lifecycle',
    'e12_onramp_quotes': 'fiat_onramp_quotes',
    'e13_remittance_quotes': 'remittance_quotes',
    'e14_l2_preconf': 'l2_preconfirmation_checks',
    'e15_fee_estimators': 'bitcoin_fee_recommendations',
    'e16_dex_routes': 'ethereum_swap_routes',
    'e17_perp_depth': 'perpetual_order_book_depth',
    'e18_attestation_pool': 'ethereum_attestation_observations',
    'e20_stratum_jobs_direct': 'bitcoin_mining_pool_jobs',
    'e21_btc_block_propagation': 'bitcoin_block_announcements',
    'e21_btc_tx_propagation': 'bitcoin_transaction_announcements',
    'e21_btc_relay_floor': 'bitcoin_peer_relay_fees',
    'e21_btc_p2p_peers': 'bitcoin_peer_connections',
    'e22_options_surface': 'options_surface',
    'e22_options_book': 'options_order_book',
    'e23_perp_mark_index': 'perpetual_mark_and_index_prices',
    'e24_solana_quotes': 'solana_swap_quotes',
    'e24_solana_routes': 'solana_swap_routes',
    'e25_ltc_block_propagation': 'litecoin_block_announcements',
    'e25_ltc_tx_propagation': 'litecoin_transaction_announcements',
    'e25_ltc_relay_floor': 'litecoin_peer_relay_fees',
    'e25_ltc_p2p_peers': 'litecoin_peer_connections',
    'e26_doge_block_propagation': 'dogecoin_block_announcements',
    'e26_doge_tx_propagation': 'dogecoin_transaction_announcements',
    'e26_doge_relay_floor': 'dogecoin_peer_relay_fees',
    'e26_doge_p2p_peers': 'dogecoin_peer_connections',
    'e27_bch_block_propagation': 'bitcoin_cash_block_announcements',
    'e27_bch_tx_propagation': 'bitcoin_cash_transaction_announcements',
    'e27_bch_relay_floor': 'bitcoin_cash_peer_relay_fees',
    'e27_bch_p2p_peers': 'bitcoin_cash_peer_connections',
    'e28_fee_estimator_accuracy': 'bitcoin_fee_percentile_comparisons',
    'e29_gas_estimators': 'ethereum_gas_recommendations',
    'e29_eth_block_fees': 'ethereum_block_fees',
    'e30_gas_estimator_accuracy': 'ethereum_gas_percentile_comparisons',
}


RESTORE_TAG = 'before-folder-rename-20260915'


def public_configs(product: dict, *, legacy_paths: bool = False) -> list[dict]:
    configs = []
    for storage_name in product['datasets']:
        config = {
            'config_name': PUBLIC_NAMES[storage_name],
            'data_files': [{'split': 'train', 'path': f'{storage_name if legacy_paths else PUBLIC_NAMES[storage_name]}/**/*.parquet'}],
        }
        if storage_name == product['example']:
            config['default'] = True
        configs.append(config)
    if len({item['config_name'] for item in configs}) != len(configs):
        raise ValueError('Public table names must be unique within a repository')
    if sum(item.get('default', False) for item in configs) != 1:
        raise ValueError('Each repository must have one default table')
    return configs


def storage_reference(product: dict) -> str:
    """Document old paths and the pre-migration revision for existing consumers."""
    rows = '\n'.join(f'| `{PUBLIC_NAMES[name]}` | `{name}/` |' for name in product['datasets'])
    return (
        '\n<details>\n<summary>Earlier file paths</summary>\n\n'
        'Each table is stored under a directory with the same descriptive name. The file '
        'contents and date partitions are unchanged. Scripts using an earlier directory name '
        'should use the corresponding table name below, or pin downloads to revision '
        f'`{RESTORE_TAG}` to access the original layout. Internal collector IDs are retained '
        'in the private archive and may appear in raw coverage records.\n\n'
        '| Current table and directory | Earlier directory |\n|---|---|\n' + rows + '\n\n</details>\n'
    )
