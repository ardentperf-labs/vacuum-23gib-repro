"""Ensure failed or absent measurement data cannot produce a success report."""
import importlib.util
import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('build_report',ROOT/'build-report.py')
report=importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

class ReportGateTests(unittest.TestCase):
    def test_longest_zero_run_counts_only_full_contiguous_ten_second_buckets(self):
        series = [
            {'startEpoch': 0.0, 'endEpoch': 10.0, 'completedTransactions': 0},
            {'startEpoch': 10.0, 'endEpoch': 20.0, 'completedTransactions': 0},
            {'startEpoch': 20.0, 'endEpoch': 30.0, 'completedTransactions': 1},
            {'startEpoch': 30.0, 'endEpoch': 40.0, 'completedTransactions': 0},
            {'startEpoch': 40.0, 'endEpoch': 48.0, 'completedTransactions': 0},
            {'startEpoch': 48.0, 'endEpoch': 58.0, 'completedTransactions': 0},
            {'startEpoch': 68.0, 'endEpoch': 78.0, 'completedTransactions': 0},
        ]
        all_active = report.longest_full_zero_run(series, 10.0)
        self.assertEqual(all_active['seconds'], 20.0)
        self.assertEqual(all_active['fullBuckets'], 2)
        only_vacuum = report.longest_full_zero_run(series, 10.0, 30.0, 60.0)
        self.assertEqual(only_vacuum['seconds'], 10.0)
        self.assertEqual(only_vacuum['fullBuckets'], 1)

    def fixture(self,path):
        results=path/'run/results';results.mkdir(parents=True)
        state=path/'run/.state';state.mkdir()
        files={'summary.json':{'status':{'testComplete':True},'coverage':{'valid':True}},
               'timing.json':{'testComplete':True},'client-impact.json':{},
               'client-summary.json':{'pgbenchExitCode':0,'failedTransactions':0,'validationErrors':[],
                                      'completedTransactions':10,'loggedCompletedTransactions':10}}
        for name,value in files.items():(results/name).write_text(json.dumps(value))
        (path/'run/source-manifest.json').write_text('{}')
        (state/'test-process.json').write_text(json.dumps({'status':'complete','exitCode':0}))
        report.ROOT=path;report.RESULTS=results
        return results
    def test_failed_data_never_writes_success_report(self):
        cases=[('summary.json','status',{'testComplete':False}),
               ('summary.json','coverage',{'valid':False}),
               ('timing.json','testComplete',False),
               ('client-summary.json','pgbenchExitCode',5),
               ('client-summary.json','failedTransactions',7),
               ('client-summary.json','validationErrors',['native logs incomplete']),
               ('client-summary.json','loggedCompletedTransactions',9)]
        for filename,key,value in cases:
            with self.subTest(filename=filename,key=key),tempfile.TemporaryDirectory() as directory:
                results=self.fixture(Path(directory))
                data=json.loads((results/filename).read_text());data[key]=value
                (results/filename).write_text(json.dumps(data))
                with self.assertRaisesRegex(RuntimeError,'validation failed|incomplete'):report.main()
                self.assertFalse((results/'REPORT.md').exists())
                self.assertFalse((results/'timeline.json').exists())
    def test_missing_replay_samples_are_unavailable_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            results=self.fixture(Path(directory))
            with self.assertRaisesRegex(RuntimeError,'backlog is unavailable'):report.main()
            self.assertFalse((results/'REPORT.md').exists())

    def test_success_report_integrates_replication_graph_and_client_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            results=self.fixture(root)
            storage={}
            for role,read_iops,read_mbps in (('primary',2.0,1.0),('replica',1.0,.5)):
                for disk in ('data','wal'):
                    storage[f'{role}_{disk}']={'readIops':read_iops,'writeIops':1.0,
                        'readMBps':read_mbps,'writeMBps':.5,'readAwaitMs':1.0,
                        'writeAwaitMs':2.0,'coveredSeconds':60.0}
            (results/'summary.json').write_text(json.dumps({
                'status':{'testComplete':True,'replayCaughtUp':True,'replicaLagBytesAtObservationEnd':0},
                'coverage':{'valid':True},'windows':{'vacuumAndReplay':storage}}))
            (results/'timing.json').write_text(json.dumps({'testComplete':True,
                'vacuumStart':'1970-01-01T00:16:45Z','vacuumComplete':'1970-01-01T00:17:05Z',
                'observationEnd':'1970-01-01T00:17:21Z'}))
            (results/'events.jsonl').write_text(json.dumps({
                'event':'replica_reached_vacuum_target_with_client',
                'time':'1970-01-01T00:17:08Z'})+'\n')
            (root/'run/source-manifest.json').write_text(json.dumps({
                'sourceSnapshotId':'/snapshots/data','uniqueId':'data-uid',
                'pairedWAL':{'sourceSnapshotId':'/snapshots/wal','uniqueId':'wal-uid'}}))
            def phase(start,end,tps,p95):
                return {'startEpoch':float(start),'endEpoch':float(end),'seconds':float(end-start),
                    'completedTps':tps,'latencyMs':{'p50':p95/2,'p95':p95,'p99':p95*1.2,'max':p95*1.5},
                    'executionMs':{'p50':p95/2,'p95':p95-2,'p99':p95,'max':p95*1.2},
                    'maxPendingXactAgeSeconds':1.0}
            phases={'pre_vacuum':phase(1000,1005,5.0,15.0),'vacuum':phase(1005,1025,3.0,40.0),
                'replay_catchup':phase(1025,1028,5.0,15.0),
                'recovery_verification':phase(1028,1035,5.0,15.0),
                'post_recovery_client':phase(1035,1040,5.0,15.0),
                'stop_drain':phase(1040,1041,None,15.0)}
            series=[]
            for low,count in ((1000,50),(1010,0),(1020,0),(1030,50)):
                series.append({'startEpoch':float(low),'endEpoch':float(low+10),'seconds':10.0,
                    'completedTransactions':count,'completedTps':count/10.0,
                    'latencyMs':{'p50':10.0,'p95':20.0,'p99':25.0,'max':30.0},
                    'scheduleLagMs':{'p50':0.0,'p95':1.0,'p99':2.0,'max':3.0},
                    'executionMs':{'p50':9.0,'p95':18.0,'p99':23.0,'max':27.0}})
            (results/'client-impact.json').write_text(json.dumps({
                'targetTps':5.0,'clients':10,'completionBucketSeconds':10.0,'phases':phases,
                'healthyWarmup':{'serverCommittedTransactions':1500,'serverCommittedTps':5.0,
                    'serverCounterDurationSeconds':300.0,'sourceFile':'client-warmup.json'},
                'series':series,'totalCompletedTransactions':100,'sessionSamples':100,
                'sessionSampleErrors':0,'sessionSampleMaxGapSeconds':1.1,
                'clockAlignment':{'clientStopRequest':{'dbEpoch':1040.0}}}))
            (results/'client-summary.json').write_text(json.dumps({'pgbenchExitCode':0,
                'failedTransactions':0,'validationErrors':[],'completedTransactions':100,
                'loggedCompletedTransactions':100}))
            raw=[]
            for index,epoch in enumerate((1005,1010,1015,1020,1025,1030,1035,1040),1):
                stamp=dt.datetime.fromtimestamp(epoch,dt.timezone.utc).isoformat().replace('+00:00','Z')
                raw.append({'seq':index,'primary':{'epoch':epoch,'ts':stamp,'flushLsn':f'0/{epoch+1000:X}',
                    'senders':[{'flushLsn':f'0/{epoch+900:X}'}]},
                    'replica':{'epoch':epoch,'replayLsn':f'0/{epoch+800:X}'}})
            (results/'samples.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in raw))
            (root/'timeline-events.jsonl').write_text('')
            recovery = root/'run/.state/recovery'
            recovery.mkdir(parents=True, exist_ok=True)
            (recovery/'cache-conditioning.json').write_text(json.dumps({
                'status':'complete','cacheDropSucceeded':True}))

            report.main()
            rendered=(results/'REPORT.md').read_text()
            self.assertIn('During the 300-second healthy warm-up, the primary committed **1,500 transactions (5.000 TPS',rendered)
            self.assertIn('Across the full pre-VACUUM interval (warm-up plus checkpoint and replica replay wait)',rendered)
            self.assertIn('pre_vacuum (full warm-up + checkpoint/replay interval)',rendered)
            self.assertIn('during VACUUM they show **3.000 completed TPS**',rendered)
            self.assertIn('completion LSN in **23.00 seconds from VACUUM start**',rendered)
            self.assertIn('ended **36.00 seconds from VACUUM start**',rendered)
            self.assertIn('**10 seconds during VACUUM** and **20 seconds across',rendered)
            self.assertIn('Maximum observed replay backlog across the sampled post-warm-up client window:',rendered)
            self.assertIn('latency includes schedule lag and commit wait',rendered)
            self.assertIn('client-replication.png',rendered)
            self.assertIn('client-replication.json',rendered)
            self.assertTrue((results/'client-replication.png').is_file())
            evidence=json.loads((results/'client-replication.json').read_text())
            self.assertEqual(evidence['series']['replicaReplayBacklogGiB']['validSamples'],8)

    def test_requires_timezone_and_normalizes_utc(self):
        with self.assertRaises(ValueError):report.timestamp('2026-10-09T10:00:00')
        self.assertEqual(report.timestamp('2026-10-09T10:00:00+02:00').isoformat(),'2026-10-09T08:00:00+00:00')

if __name__=='__main__':unittest.main()
