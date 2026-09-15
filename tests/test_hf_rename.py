"""Exercise copy verification, collision handling and public/private path separation."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock, patch

from huggingface_hub import CommitOperationCopy, CommitOperationDelete
import yaml

from src.ops.dataset_names import PUBLIC_NAMES, RESTORE_TAG, public_configs
from src.ops.hf_cards import rewrite_card
from src.ops.hf_push import PRODUCTS, _stage
from src.ops.hf_rename import migrate, plan

NAME = 'crypto-options-surface'


def snapshot(files, sha='before', private=False):
    return NS(sha=sha, private=private, siblings=[NS(rfilename=p, **v) for p, v in files.items()])


def original():
    return {f'{table}/2026/09/sample.parquet': {'blob_id': f'blob-{table}', 'size': 123, 'lfs': None}
            for table in PRODUCTS[NAME]['datasets']} | {'.gitattributes': {'blob_id': 'attrs', 'size': 20, 'lfs': None}}


class RenameTests(unittest.TestCase):
    def test_copy_then_verify_then_delete_exact_originals(self):
        files = original()
        before = snapshot(files)
        _, moves, expected = plan(before, NAME)
        copied = files | {target: files[source] for source, target in moves.items()}
        api = Mock()
        api.list_repo_refs.return_value = NS(tags=[])
        api.create_commit.side_effect = [NS(oid='copy'), NS(oid='final')]
        api.dataset_info.side_effect = [snapshot(copied, 'copy'), snapshot(expected, 'final')]
        with TemporaryDirectory() as tmp:
            receipt = migrate(api, NAME, before, b'card', Path(tmp))
        self.assertTrue(receipt['file_contents_preserved'])
        self.assertEqual(len(files), len(expected))
        calls = api.create_commit.call_args_list
        self.assertEqual(calls[0].kwargs['parent_commit'], 'before')
        self.assertTrue(all(isinstance(op, CommitOperationCopy) for op in calls[0].kwargs['operations']))
        self.assertTrue(all(op.src_revision == 'before' for op in calls[0].kwargs['operations']))
        self.assertEqual(calls[1].kwargs['parent_commit'], 'copy')
        deletes = [op.path_in_repo for op in calls[1].kwargs['operations'] if isinstance(op, CommitOperationDelete)]
        self.assertEqual(set(deletes), set(moves))
        self.assertEqual(api.create_tag.call_args.kwargs['revision'], 'before')
        self.assertLess([c[0] for c in api.mock_calls].index('dataset_info'),
                        max(i for i,c in enumerate(api.mock_calls) if c[0] == 'create_commit'))

    def test_bad_copy_stops_before_any_deletion(self):
        api = Mock()
        api.list_repo_refs.return_value = NS(tags=[])
        api.create_commit.return_value = NS(oid='copy')
        api.dataset_info.return_value = snapshot(original(), 'copy')
        with TemporaryDirectory() as tmp, self.assertRaises(RuntimeError):
            migrate(api, NAME, snapshot(original()), b'card', Path(tmp))
        self.assertEqual(api.create_commit.call_count, 1)
        self.assertTrue(all(isinstance(op, CommitOperationCopy) for op in api.create_commit.call_args.kwargs['operations']))

    def test_collision_refused_and_identical_partial_copy_resumable(self):
        files = original()
        _, moves, expected = plan(snapshot(files), NAME)
        source, target = next(iter(moves.items()))
        files[target] = deepcopy(files[source])
        self.assertEqual(plan(snapshot(files), NAME)[2], expected)
        files[target]['blob_id'] = 'different-data'
        with self.assertRaises(ValueError):
            plan(snapshot(files), NAME)

    def test_refuses_private_unknown_and_missing_tables(self):
        with self.assertRaises(ValueError):
            plan(snapshot(original(), private=True), NAME)
        with self.assertRaises(ValueError):
            plan(snapshot({'unknown/data.parquet': next(iter(original().values()))}), NAME)
        with self.assertRaises(ValueError):
            plan(snapshot({}), NAME)

    def test_completed_migration_is_resumable_without_replacing_restore_tag(self):
        expected = plan(snapshot(original()), NAME)[2]
        api = Mock()
        api.list_repo_refs.return_value = NS(tags=[NS(name=RESTORE_TAG)])
        api.create_commit.return_value = NS(oid='final')
        api.dataset_info.return_value = snapshot(expected)
        with TemporaryDirectory() as tmp:
            migrate(api, NAME, snapshot(expected), b'card', Path(tmp))
        api.create_tag.assert_not_called()
        self.assertEqual(api.create_commit.call_count, 1)
        self.assertFalse(any(isinstance(op, CommitOperationDelete) for op in api.create_commit.call_args.kwargs['operations']))

    def test_legacy_card_paths_update_without_changing_other_metadata(self):
        metadata = {'license': 'odc-by', 'pretty_name': 'Old title', 'extra': 'preserved',
                    'configs': public_configs(PRODUCTS[NAME], legacy_paths=True)}
        card = ('---\n' + yaml.safe_dump(metadata) + '---\nOld body').encode()
        after = yaml.safe_load(rewrite_card(card, NAME).decode().split('---')[1])
        self.assertEqual(after['configs'], public_configs(PRODUCTS[NAME]))
        self.assertEqual(after['extra'], 'preserved')

    def test_staging_renames_public_paths_and_preserves_private_paths_and_bytes(self):
        table = 'e0_run_manifest'
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / 'data'
            source = data / table / '2026/09/sample.parquet'
            source.parent.mkdir(parents=True)
            source.write_bytes(b'original-data')
            with patch('src.ops.hf_push.DATA', data):
                for folder, window, dirname in [('public', ('2026-09-01', '2026-09-07'), PUBLIC_NAMES[table]),
                                               ('private', None, table)]:
                    _stage(root / folder, [table], window, None)
                    self.assertEqual((root / folder / dirname / '2026/09/sample.parquet').read_bytes(), b'original-data')
            self.assertEqual(source.read_bytes(), b'original-data')


if __name__ == '__main__':
    unittest.main()
