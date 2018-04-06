#!/usr/bin/env python

# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import sys
import os


def call(cmd):
    print '+', cmd
    status = os.system(cmd)
    if status:
        raise OSError('invocation failed')


def main():
    OPTIONS = get_options(sys.argv[1:])
    call('time python make_db.py --buckets %s --junit --threads 32' % OPTIONS.buckets)

    bq_cmd = 'bq load --source_format=NEWLINE_DELIMITED_JSON --max_bad_records=1000'
    mj_cmd = 'pypy make_json.py'

    mj_ext = ''
    bq_ext = ''
    try:
        call(mj_cmd + ' --days 1 --assert-oldest 1.9')
    except OSError:
        # cycle daily/weekly tables
        bq_ext = ' --replace'
        mj_ext = ' --reset-emitted'

    call(mj_cmd + mj_ext + ' --days 1 | pv | gzip > build_day.json.gz')
    call(bq_cmd + bq_ext + ' %s:build.day build_day.json.gz schema.json' % OPTIONS.bq_instance)

    call(mj_cmd + mj_ext + ' --days 7 | pv | gzip > build_week.json.gz')
    call(bq_cmd + bq_ext + ' %s:build.week build_week.json.gz schema.json' % OPTIONS.bq_instance)

    call(mj_cmd + ' | pv | gzip > build_all.json.gz')
    call(bq_cmd + ' %s:build.all build_all.json.gz schema.json' % OPTIONS.bq_instance)

    # TODO (chx) remove the hardcoding
    call('python stream.py --poll kubernetes-jenkins/gcs-changes/kettle '
         ' --dataset %s:build --tables all:0 day:1 week:7 --stop_at=1' % OPTIONS.bq_instance)

def get_options(argv):
    """Process command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--buckets',
        help='YAML file with GCS bucket locations',
        default='../buckets.yaml',
        required=True,
    )
    parser.add_argument(
        '--bq_instance',
        help='The Big Query instance to which results are uploaded',
        default='k8s-gubernator',
        required=True,
    )
    return parser.parse_args(argv)

if __name__ == '__main__':
    os.chdir(os.path.dirname(__file__))
    os.environ['TZ'] = 'America/Los_Angeles'
    main()
