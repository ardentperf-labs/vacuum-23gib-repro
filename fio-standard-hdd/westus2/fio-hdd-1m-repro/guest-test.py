#!/usr/bin/env python3
"""Run as root on the disposable VM. All test-disk operations are reads."""
import argparse, datetime as dt, json, os, pathlib, resource, subprocess, time
parser = argparse.ArgumentParser()
parser.add_argument('--single-validation', action='store_true')
parser.add_argument('--only-threads', type=int, nargs='+')
opts = parser.parse_args()
ROOT = pathlib.Path('/home/fioadmin/repeat-single' if opts.single_validation else '/home/fioadmin/results')
ROOT.mkdir(exist_ok=True)
(ROOT/'complete.json').unlink(missing_ok=True)
os.environ['LC_ALL'] = 'C'
os.environ['TZ'] = 'UTC'
resource.setrlimit(resource.RLIMIT_NOFILE, (262144, 262144))
def run(*args):
    return subprocess.check_output(args, text=True)
def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()
# Prefer Azure LUN 0 links. Newer VM sizes may expose managed disks via NVMe.
links = [pathlib.Path('/dev/disk/azure/scsi1/lun0'),
         pathlib.Path('/dev/disk/azure/data/by-lun/0')]
device = next((p.resolve() for p in links if p.exists()), None)
if device is None:
    disks = json.loads(run('lsblk', '--json', '--bytes', '--output',
                           'NAME,PATH,SIZE,TYPE,FSTYPE,MOUNTPOINTS'))['blockdevices']
    candidates = [d for d in disks if d['type'] == 'disk'
                  and int(d['size']) == 512 * 1024**3 and d['fstype'] == 'ext4'
                  and not any(d.get('mountpoints') or []) and not d.get('children')]
    assert len(candidates) == 1, 'Cannot uniquely identify the prepared data disk'
    device = pathlib.Path(candidates[0]['path'])
assert device.exists(), device
assert int(run('blockdev', '--getsize64', str(device))) == 512 * 1024**3
assert run('blkid', '-s', 'TYPE', '-o', 'value', str(device)).strip() == 'ext4'
subprocess.run(['blockdev', '--setro', str(device)], check=True)
assert run('blockdev', '--getro', str(device)).strip() == '1'
mount = pathlib.Path('/mnt/fio-data')
mount.mkdir(exist_ok=True)
if not os.path.ismount(mount):
    subprocess.run(['mount', '-o', 'ro,noload', str(device), str(mount)], check=True)
files = sorted((p for p in mount.rglob('*') if p.is_file() and p.stat().st_size >= 1024**3),
               key=lambda p: p.stat().st_size, reverse=True)[:16]
assert files, 'No populated large files found; refusing to read untouched disk space'
# PostgreSQL relation segments are physically allocated, unlike a sparse raw disk.
for p in files:
    assert p.stat().st_blocks * 512 >= p.stat().st_size * .95, (str(p), p.stat())
config = dict(block_size_bytes=1024**2, block_size='1 MiB', engine='psync', iodepth=1,
              device=str(device),files=[dict(path=str(p),size=p.stat().st_size,
              allocated=p.stat().st_blocks*512) for p in files],start=stamp())
(ROOT / 'config.json').write_text(json.dumps(config, indent=2))
guest = run('sh', '-c', 'fio --version; uname -a; lsblk -o NAME,SIZE,TYPE,MOUNTPOINTS; '
            'findmnt /mnt/fio-data; free -m; lscpu; '
            f'cat /sys/block/{device.name}/queue/scheduler; '
            f'cat /sys/block/{device.name}/device/queue_depth; '
            f'cat /sys/block/{device.name}/queue/nr_requests; '
            f'for f in max_sectors_kb max_hw_sectors_kb max_segments logical_block_size physical_block_size read_ahead_kb; do '
            f'echo "$f=$(cat /sys/block/{device.name}/queue/$f)"; done; '
            'cat /proc/sys/kernel/threads-max; cat /proc/sys/kernel/pid_max')
(ROOT / 'guest.txt').write_text(guest)
counts = [1] if opts.single_validation else (opts.only_threads or [1,2,4,8,16,32,64,128,144,160,256,512,768,1024])
for threads in counts:
    dest = ROOT / f'run-{threads}'
    if (dest / 'meta.json').exists():
        if json.loads((dest / 'meta.json').read_text()).get('returncode', 0) == 0:
            continue
        dest.rename(ROOT / f'run-{threads}-failed-{int(time.time())}')
    dest.mkdir(exist_ok=True)
    # Start at a minute boundary; ramp 30s then measure 180s. Collection uses
    # the same conservative leading Azure bucket guard as the 4 KiB sweep.
    time.sleep(60 - time.time() % 60)
    start = time.time()
    meta = dict(threads=threads,start=stamp(),
                measurement_start=dt.datetime.fromtimestamp(start+30,dt.timezone.utc).isoformat(),
                measurement_end=dt.datetime.fromtimestamp(start+210,dt.timezone.utc).isoformat())
    ioout = (dest / 'iostat.json').open('w')
    cpuout = (dest / 'vmstat.txt').open('w')
    io = subprocess.Popen(['iostat','-x','-d','-y','-t','-o','JSON','1'], stdout=ioout)
    cpu = subprocess.Popen(['vmstat','-t','1'],stdout=cpuout)
    cmd = ['fio','--name=read-scaling','--readonly','--rw=randread','--bs=1m',
           '--ioengine=psync','--iodepth=1','--direct=1','--thread=1',
           '--numjobs='+str(threads),'--group_reporting=1',
           '--filename='+':'.join(map(str,files)),'--file_service_type=random',
           '--norandommap=1','--randrepeat=0','--invalidate=0','--time_based=1',
           '--ramp_time=30','--runtime=180','--output-format=json',
           '--output='+str(dest/'fio.json')]
    (dest/'command.json').write_text(json.dumps(cmd, indent=2))
    with (dest/'fio.stderr').open('w') as err:
        result = subprocess.run(cmd,stderr=err)
    # SIGINT lets iostat close its JSON document.
    io.send_signal(2); cpu.terminate()
    io.wait(); cpu.wait(); ioout.close(); cpuout.close()
    meta.update(end=stamp(),returncode=result.returncode)
    (dest/'meta.json').write_text(json.dumps(meta,indent=2))
    assert result.returncode == 0, f'fio failed with {threads} threads'
    jobs = json.loads((dest/'fio.json').read_text())['jobs']
    assert all(j['error'] == 0 and j['write']['io_bytes'] == 0 for j in jobs)
    read = jobs[0]['read']
    print(json.dumps(dict(threads=threads,iops=read['iops'],mean_ms=read['lat_ns']['mean']/1e6)),flush=True)
    # User requested two completed points after 512: 768 and 1,024.
    # Complete every requested phase regardless of its measured latency.
(ROOT/'complete.json').write_text(json.dumps(dict(end=stamp())))
