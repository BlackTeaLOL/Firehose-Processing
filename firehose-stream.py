#!/bin/python3

#           DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
#                   Version 2, December 2004
# 
# Copyright (C) 2023 William Welna
#
# Everyone is permitted to copy and distribute verbatim or modified
# copies of this license document, and changing it is allowed as long
# as the name is changed.
# 
#           DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
#  TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION
#
# 0. You just DO WHAT THE FUCK YOU WANT TO.


from atproto import CAR, AtUri
from atproto.firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from atproto.firehose.models import MessageFrame
from atproto.xrpc_client import models
from atproto.xrpc_client.models import ids

import json
import pprint
import redis
import time
import datetime
from string import Template
import subprocess

class RotateFile:

    # "test-$d.log"
    def __init__(self, file_template, path=None, hours=12, mode='a+', encoding='utf-8'):
        self.dt = datetime.datetime.utcnow()
        self.template = Template(file_template)
        self.path = path
        self.mode = mode
        self.hours = hours
        self.encoding = encoding
        self.location = self._do_template(self.dt)

        self.file = open(self.location, mode=mode, encoding=encoding)
    
    def _do_template(self, dt) -> str:
        ret = self.template.substitute({'d': dt.strftime('%m%d%Y-%H%M%S')})
        if self.path != None:
            ret = f"{self.path}{ret}"
        return ret
    
    def _datecheck(self) -> None:
        if self.dt < datetime.datetime.utcnow() - datetime.timedelta(hours=self.hours):
            self.file.close()
            subprocess.Popen(['xz', '-9e', self.location])

            self.dt = datetime.datetime.utcnow()
            self.location = self._do_template(self.dt)
            self.file = open(self.location, mode=self.mode, encoding=self.encoding)

    def flush(self)-> None: self.file.flush()
    def write(self, d) -> int:
        self._datecheck()
        return self.file.write(d)

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()
        subprocess.Popen(['xz', '-9e', self.location])

def walk(q):
    if isinstance(q, dict):
        ret = {}
        for k in q.keys():
            if isinstance(q[k], (int, str, float, type(True), type(False), type(None))):
                ret[k] = q[k]
                continue
            if isinstance(q[k], dict) or isinstance(q[k], list):
                ret[k] = walk(q[k])
                continue
            ret[k] = str(q[k])
        return ret
    elif isinstance(q, list):
        ret = []
        for e in q:
            if isinstance(e, (int, str, float, type(True), type(False), type(None))):
                ret.append(e)
                continue
            if isinstance(e, (dict, list)):
                ret.append(walk(e))
                continue
            ret.append(str(e))
        return ret

def sort_records(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> list: 
    records = []

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')
        
        if not op.cid:
            continue

        record_raw_data = car.blocks.get(op.cid)
        if not record_raw_data:
            continue

        records.append({
            'date': commit.time,
            'operation': op.action,
            'cid': str(op.cid),
            'type': record_raw_data['$type'],
            'uri': str(uri),
            'repo': commit.repo,
            'record': walk(record_raw_data)
        })

    return records

def main():
    #benchmark_times = []
    try:
        client = FirehoseSubscribeReposClient()
        r = redis.Redis()

        with RotateFile("BlueSkyStream-$d.json") as ou:
            def on_message_handler(message: MessageFrame) -> None:
                commit = parse_subscribe_repos_message(message)
                if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                    return

                ops = sort_records(commit)
                
                #start = time.time()
                records = []
                for record in ops:
                    r.lpush("BlueSky-Firehose", json.dumps(record))
                    ou.write(f"{json.dumps(record)}\n")

                #end = time.time()
                #benchmark_times.append(json.dumps({'len':len(ops), 'time':end - start}))
            client.start(on_message_handler)
    except KeyboardInterrupt:
        #for x in benchmark_times:
        #    pprint.pprint(x)
        quit()

errors = []
if __name__ == '__main__':
        while True:
            try: main()
            except Exception as e:
                errors.append({'t':time.time(), 'e':str(e)})
                print(f"{datetime.datetime.now()} -> Got Error {str(e)}")
                
                dt_off = datetime.datetime.timestamp(datetime.datetime.now() + datetime.timedelta(hours=3))
                
                new_list = []
                for e in errors:
                    if e['t'] < dt_off:
                        new_list.append(e)
                
                errors = new_list

                if len(errors) > 5:
                    for e in errors:
                        pprint.pprint(e)
                    quit(-1)
                
                time.sleep(7)

