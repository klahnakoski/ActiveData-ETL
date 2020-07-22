
# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from activedata_etl import etl2key, key2etl
from activedata_etl.imports.text_log import process_text_log
from activedata_etl.transforms import EtlHeadGenerator
from activedata_etl.transforms.pulse_block_to_es import scrub_pulse_record, transform_buildbot
from mo_dots import Data, wrap, coalesce
from mo_logs import Log
from mo_times.dates import Date
from mo_times.timer import Timer
from pyLibrary.env import git
from mo_http import http

DEBUG = False


def process(source_key, source, dest_bucket, resources, please_stop=None):
    etl_head_gen = EtlHeadGenerator(source_key)
    stats = Data()
    counter = 0
    output = []

    for i, pulse_line in enumerate(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        pulse_record = scrub_pulse_record(source_key, i, pulse_line, stats)
        if not pulse_record:
            continue
        pulse_record.etl.source.id = key2etl(source_key).source.id

        etl = wrap({
            "id": counter,
            "file": pulse_record.payload.logurl,
            "timestamp": Date.now().unix,
            "revision": git.get_revision(),
            "source": {
                "id": 0,
                "source": pulse_record.etl,
                "type": "join"
            },
            "type": "join"
        })

        if pulse_record.payload.what == "This is a heartbeat":  # RECORD THE HEARTBEAT, OTHERWISE SOMEONE WILL ASK WHERE THE MISSING RECORDS ARE
            data = Data(etl=etl)
            data.etl.error = "Pulse Heartbeat"
            output.append(data)
            counter += 1
            continue

        data = transform_buildbot(source_key, pulse_record.payload, resources)
        data.etl = etl
        with Timer("Read {{url}}", {"url": pulse_record.payload.logurl}, silent=not DEBUG) as timer:
            try:
                if pulse_record.payload.logurl == None:
                    data.etl.error = "No logurl"
                    output.append(data)
                    continue
                response = http.get(
                    url=pulse_record.payload.logurl,
                    retry={"times": 3, "sleep": 10}
                )
                if response.status_code == 404:
                    Log.note("Text log does not exist {{url}}", url=pulse_record.payload.logurl)
                    data.etl.error = "Text log does not exist"
                    output.append(data)
                    continue
                all_log_lines = response.get_all_lines(encoding=None)
                data.action = process_text_log(all_log_lines, pulse_record.payload.logurl)

                verify_equal(data, "build.revision", "action.revision", from_url=pulse_record.payload.logurl, from_key=source_key)
                verify_equal(data, "build.id", "action.buildid", from_url=pulse_record.payload.logurl, from_key=source_key)
                verify_equal(data, "run.machine.name", "action.subordinate", from_url=pulse_record.payload.logurl, from_key=source_key)

                output.append(data)
                Log.note("Found builder record for id={{id}}", id=etl2key(data.etl))
            except Exception as e:
                Log.warning("Problem processing {{url}}", url=pulse_record.payload.logurl, cause=e)
                data.etl.error = "Text log unreachable"
                output.append(data)
            finally:
                counter += 1
                etl_head_gen.next_id = 0

        data.etl.duration = timer.duration

    dest_bucket.extend([{"id": etl2key(d.etl), "value": d} for d in output])
    return {source_key + ".0"}


def verify_equal(data, expected, duplicate, warning=True, from_url=None, from_key=None):
    """
    WILL REMOVE duplicate IF THE SAME
    """
    if data[expected] == data[duplicate]:
        data[duplicate] = None
    elif data[expected] == None or data[expected] == "None":
        data[expected] = data[duplicate]
        data[duplicate] = None
    elif data[expected] and data[duplicate] and data[duplicate] in data[expected]:
        data[duplicate] = None
    elif data[expected] and data[duplicate] and data[expected] in data[duplicate]:
        data[expected] = data[duplicate]
        data[duplicate] = None
    elif data[expected] and not data[duplicate]:
        data[duplicate] = None
    else:
        if warning:
            frum = coalesce(from_url, from_key, "<unknown>")
            Log.warning("{{a}} != {{b}} ({{av|json}}!={{bv|json}}) in {{url}}", a=expected, b=duplicate, av=data[expected], bv=data[duplicate], url=frum)


if __name__ == "__main__":
    response = http.get("http://archive.mozilla.org/pub/b2g/tinderbox-builds/mozilla-central-emulator/1453460790/mozilla-central_ubuntu64_vm-b2g-emulator_test-mochitest-8-bm52-tests1-linux64-build4.txt.gz")
    # response = http.get("http://ftp.mozilla.org/pub/mozilla.org/firefox/tinderbox-builds/mozilla-inbound-win32/1444321537/mozilla-inbound_xp-ix_test-g2-e10s-bm119-tests1-windows-build710.txt.gz")
    # for i, l in enumerate(response._all_lines(encoding=None)):
    #     Log.note("{{line}}", line=l)

    try:
        data = process_text_log(response.all_lines, "<unknown>")
        Log.note("{{data}}", data=data)
    finally:
        response.close()
