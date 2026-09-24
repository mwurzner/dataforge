from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace as NS
import hashlib
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow.parquet as pq

from src.ephemeral.market_research import ladder, take_cost
from src.ops.market_research import check_budget, publish, DAY_BUDGET, DATASET
from src.ops.collection_health import summarize
from src.ephemeral.e31_btc_fee_benchmark import Benchmark, DATASETS


class ResearchTests(unittest.TestCase):
    def test_depth_cost_sorts_prices_and_refuses_insufficient_size(self):
        asks=[{'price':'0.6','size':'10'},{'price':'0.4','size':'5'}]
        self.assertEqual(take_cost(asks,10),'5.0')
        self.assertIsNone(take_cost(asks,16))
        self.assertIsNone(take_cost([],1))

    def test_option_ladder_pairs_three_strikes_and_two_expiries(self):
        rows=[dict(instrument_name=f'{day}-{strike}-{kind}',kind='option',is_active=True,
                   expiration_timestamp=day*86400*1000,strike=strike,option_type=kind)
              for day in (1,3,7,30) for strike in (80,90,100,110,120) for kind in ('call','put')]
        result=ladder(rows,100,0)
        self.assertEqual(len(result),12)
        self.assertEqual({i['strike'] for i in result},{90,100,110})
        self.assertEqual({i['expiration_timestamp']/86400000 for i in result},{3,7})

    def test_budget_stops_without_pruning_or_overwriting(self):
        remote=f'{DATASET}/2026/09/24/new.parquet'
        info=NS(private=True,used_storage=100,siblings=[NS(rfilename=f'{DATASET}/2026/09/24/old.parquet',size=DAY_BUDGET-10)])
        with self.assertRaises(ValueError):check_budget(info,remote,11)
        self.assertEqual(check_budget(info,remote,10)['day_before_bytes'],DAY_BUDGET-10)
        info.private=False
        with self.assertRaises(ValueError):check_budget(info,remote,1)

    def test_budget_or_collision_prevents_any_remote_mutation(self):
        api=Mock()
        remote=f'{DATASET}/2026/09/24/existing.parquet'
        api.dataset_info.return_value=NS(private=True,used_storage=100,siblings=[NS(rfilename=remote,size=1)])
        with TemporaryDirectory() as tmp:
            file=Path(tmp)/'new.parquet';file.write_bytes(b'data')
            with self.assertRaises(ValueError):publish(api,file,remote)
        api.create_commit.assert_not_called()

    def test_health_exposes_row_errors_and_missing_panels(self):
        rows=summarize({'gas':[pd.DataFrame({'provider':['a','a','b'],'error':[None,'HTTP429',None]})], 'options':[]})
        a=next(r for r in rows if r['provider']=='a')
        self.assertEqual(a['status'],'degraded');self.assertEqual(a['error_rows'],1)
        self.assertEqual(rows[-1]['status'],'no_observations')

    def test_publication_adds_one_file_and_verifies_existing_identities(self):
        remote=f'{DATASET}/2026/09/24/new.parquet'
        data=b'new observation'
        old=NS(rfilename='existing/data.parquet',size=10,blob_id='old-blob',lfs=None)
        new=NS(rfilename=remote,size=len(data),blob_id='new-blob',
               lfs=NS(sha256=hashlib.sha256(data).hexdigest()))
        before=NS(private=True,used_storage=10,sha='expected-parent',siblings=[old])
        after=NS(private=True,siblings=[old,new])
        api=Mock()
        api.dataset_info.side_effect=[before,after]
        api.create_commit.return_value=NS(oid='new-commit')
        with TemporaryDirectory() as tmp:
            file=Path(tmp)/'new.parquet';file.write_bytes(data)
            receipt=publish(api,file,remote)
        self.assertTrue(receipt['existing_files_unchanged'])
        kwargs=api.create_commit.call_args.kwargs
        self.assertEqual(kwargs['parent_commit'],'expected-parent')
        self.assertEqual(len(kwargs['operations']),1)
        self.assertEqual(kwargs['operations'][0].path_in_repo,remote)

    def test_publication_verification_detects_changed_existing_file(self):
        remote=f'{DATASET}/2026/09/24/new.parquet'
        data=b'new observation'
        old=NS(rfilename='existing/data.parquet',size=10,blob_id='old-blob',lfs=None)
        changed=NS(rfilename=old.rfilename,size=10,blob_id='changed-blob',lfs=None)
        new=NS(rfilename=remote,size=len(data),blob_id='new-blob',
               lfs=NS(sha256=hashlib.sha256(data).hexdigest()))
        api=Mock()
        api.dataset_info.side_effect=[
            NS(private=True,used_storage=10,sha='expected-parent',siblings=[old]),
            NS(private=True,siblings=[changed,new])]
        api.create_commit.return_value=NS(oid='new-commit')
        with TemporaryDirectory() as tmp:
            file=Path(tmp)/'new.parquet';file.write_bytes(data)
            with self.assertRaises(RuntimeError):publish(api,file,remote)

    def test_hourly_checkpoints_keep_all_rows_and_old_files(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            old=root/DATASETS[0]/'2026/09/original.parquet'
            old.parent.mkdir(parents=True);old.write_bytes(b'untouched-old-file')
            b=Benchmark(root,'2026-09-24T100000Z',client=Mock())
            for number in (1,2,3):
                b.rows[DATASETS[0]].append({'quote_id':str(number),'sat_per_vb':float(number)})
                b.checkpoint(str(number))
            files=[p for p in (root/DATASETS[0]).rglob('*.parquet') if p!=old]
            frames=[pq.ParquetFile(p).read().to_pandas() for p in files]
            self.assertEqual(pd.concat(frames).quote_id.tolist(),['1','2','3'])
            self.assertEqual(old.read_bytes(),b'untouched-old-file')
            self.assertTrue(all('hourly' in p.name for p in files))

    def test_quote_source_failure_preserves_other_results(self):
        from src.ops import longpoll as lp
        frame=pd.DataFrame({'error':[None]})
        with (patch.object(lp.e10_quotes,'sample',side_effect=RuntimeError('failed')),
             patch.object(lp.e22_options_surface,'sample',return_value=frame),
             patch.object(lp.e22_options_surface,'books',return_value=frame),
             patch.object(lp.e17_perpdepth,'sample',return_value=frame),
             patch.object(lp.e23_perp_mark,'sample',return_value=frame),
             patch.object(lp.e24_solana_quotes,'sample',return_value=(frame,frame)),
             patch.object(lp.e12_onramp,'sample',return_value=frame),
             patch.object(lp.e13_remit,'sample',return_value=frame)):
            result=lp._quote_round()
        self.assertEqual(len(result[0]),0)
        self.assertEqual(len(result[5]),1)
        self.assertTrue(result[-1])


if __name__=='__main__':
    unittest.main()
