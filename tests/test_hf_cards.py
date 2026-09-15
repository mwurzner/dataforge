"""Verify that card publication cannot include dataset writes or deletions."""
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from huggingface_hub import CommitOperationAdd
import yaml

from src.ops.hf_cards import file_identities, publish_one, rewrite_card, validate_config_files
from src.ops.hf_push import PRODUCTS, _card
from src.ops.dataset_names import PUBLIC_NAMES, public_configs


def info(sha='before', blob='data-blob', private=False):
    return SimpleNamespace(sha=sha, private=private, siblings=[
        SimpleNamespace(rfilename='README.md', blob_id='card-blob', size=12, lfs=None),
        SimpleNamespace(rfilename='observations/2026/09/data.parquet', blob_id=blob, size=1024, lfs=None),
    ])


class CardTests(unittest.TestCase):
    def test_preserves_metadata_and_table_names_for_every_product(self):
        for name, product in PRODUCTS.items():
            with self.subTest(name=name):
                existing = _card(name, product).replace('license: odc-by', 'license: odc-by\nextra_metadata: keep-me')
                result = rewrite_card(existing.encode(), name).decode()
                metadata = yaml.safe_load(result.split('---')[1])
                self.assertEqual(metadata['extra_metadata'], 'keep-me')
                self.assertEqual(metadata['license'], 'odc-by')
                self.assertEqual(metadata['tags'], product['tags'])
                self.assertEqual(metadata['pretty_name'], product['pretty'])
                self.assertEqual(metadata['configs'], public_configs(product))
                for table in product['datasets']:
                    self.assertIn(f'`{PUBLIC_NAMES[table]}`', result)
                # Validate Python examples without fetching data.
                for example in result.split('```python\n')[1:]:
                    compile(example.split('```', 1)[0], name, 'exec')

    def test_refuses_unexpected_metadata(self):
        for card in (b'no front matter', b'---\nlicense: proprietary\n---\ntext'):
            with self.assertRaises(ValueError):
                rewrite_card(card, 'bitcoin-mempool-lifecycle')

    def test_new_names_select_descriptive_directories_and_have_one_default(self):
        all_tables = {table for product in PRODUCTS.values() for table in product['datasets']}
        self.assertEqual(set(PUBLIC_NAMES), all_tables)
        self.assertEqual(len(set(PUBLIC_NAMES.values())), len(PUBLIC_NAMES))
        for product in PRODUCTS.values():
            configs = public_configs(product)
            self.assertEqual(sum(config.get('default', False) for config in configs), 1)
            for table, config in zip(product['datasets'], configs):
                self.assertEqual(config['data_files'], [{'split': 'train', 'path': f'{PUBLIC_NAMES[table]}/**/*.parquet'}])
                self.assertNotRegex(config['config_name'], r'^e\d+_')

    def test_existing_custom_configs_are_not_overwritten(self):
        card = b'---\nlicense: odc-by\npretty_name: example\nconfigs:\n- config_name: custom\n  data_files: custom.parquet\n---\ntext'
        with self.assertRaises(ValueError):
            rewrite_card(card, 'crypto-options-surface')

    def test_existing_card_without_configs_gets_names_idempotently(self):
        card = b'---\nlicense: odc-by\npretty_name: example\nextra_metadata: preserved\n---\ntext'
        updated = rewrite_card(card, 'crypto-options-surface')
        self.assertEqual(updated, rewrite_card(updated, 'crypto-options-surface'))
        self.assertEqual(yaml.safe_load(updated.decode().split('---')[1])['extra_metadata'], 'preserved')

    def test_config_validation_refuses_missing_or_uncovered_partitions(self):
        name = 'crypto-options-surface'
        files = [SimpleNamespace(rfilename=f'{PUBLIC_NAMES[table]}/2026/09/sample.parquet') for table in PRODUCTS[name]['datasets']]
        valid = SimpleNamespace(siblings=files)
        validate_config_files(valid, name)
        with self.assertRaises(ValueError):
            validate_config_files(SimpleNamespace(siblings=files[:-1]), name)
        with self.assertRaises(ValueError):
            validate_config_files(SimpleNamespace(siblings=files + [SimpleNamespace(rfilename='unmapped/2026/09/sample.parquet')]), name)

    def test_single_readme_operation_with_parent_lock_and_hash_verification(self):
        api = Mock()
        api.create_commit.return_value = SimpleNamespace(oid='after')
        api.dataset_info.return_value = info('after')
        with TemporaryDirectory() as tmp:
            result = publish_one(api, 'dataforge-labs/example', info(), b'edited card', Path(tmp) / 'receipt.json')
        call = api.create_commit.call_args.kwargs
        self.assertEqual(call['parent_commit'], 'before')
        self.assertEqual(len(call['operations']), 1)
        operation = call['operations'][0]
        self.assertIsInstance(operation, CommitOperationAdd)
        self.assertEqual(operation.path_in_repo, 'README.md')
        self.assertEqual(operation.path_or_fileobj, b'edited card')
        self.assertTrue(result['non_card_files_unchanged'])
        api.dataset_info.assert_called_once_with('dataforge-labs/example', revision='after', files_metadata=True)

    def test_aborts_when_non_card_file_changes(self):
        api = Mock()
        api.create_commit.return_value = SimpleNamespace(oid='after')
        api.dataset_info.return_value = info('after', blob='unexpected-change')
        with TemporaryDirectory() as tmp, self.assertRaises(RuntimeError):
            publish_one(api, 'dataforge-labs/example', info(), b'card', Path(tmp) / 'receipt.json')

    def test_refuses_private_repository_before_any_write(self):
        api = Mock()
        with TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            publish_one(api, 'dataforge-labs/example', info(private=True), b'card', Path(tmp) / 'receipt.json')
        api.create_commit.assert_not_called()

    def test_refuses_incomplete_file_metadata(self):
        before = info()
        before.siblings[1].blob_id = None
        with self.assertRaises(ValueError):
            file_identities(before)


if __name__ == '__main__':
    unittest.main()
