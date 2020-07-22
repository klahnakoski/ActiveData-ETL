# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from activedata_etl import etl2key
from activedata_etl.imports.buildbot import BuildbotTranslator
from activedata_etl.imports.resource_usage import normalize_resource_usage
from activedata_etl.transforms import TRY_AGAIN_LATER
from activedata_etl.transforms.pulse_block_to_job_logs import verify_equal, process_text_log
from jx_elasticsearch import elasticsearch
from mo_dots import Data, set_default
from mo_dots import coalesce
from mo_hg.hg_mozilla_org import minimize_repo
from mo_json import json2value
from mo_logs import Log
from mo_logs.exceptions import Except
from mo_times.dates import Date
from mo_times.durations import MONTH
from mo_times.timer import Timer
from pyLibrary.env import git
from mo_http import http

DEBUG = False
TOO_OLD = (Date.today() - 3 * MONTH).unix


def process(source_key, source, dest_bucket, resources, please_stop=None):
    bb = BuildbotTranslator()
    output = []

    for buildbot_line in list(source.read_lines()):
        if please_stop:
            Log.error("Shutdown detected. Stopping job ETL.")

        buildbot_data = json2value(buildbot_line)
        try:
            data = bb.parse(buildbot_data.builds)
        except Exception as e:
            Log.error(
                "Can not parse\n{{details|json|indent}}",
                details=buildbot_data,
                cause=e
            )

        # RESOURCE USAGE
        try:
            for a in data.run.files:
                if a.name == "resource-usage.json":
                    data.resource_usage = normalize_resource_usage(a.url)
                    break
        except Exception as e:
            Log.warning("Could not process resource-usage.json for key={{key}}", key=source_key, cause=e)

        if data.action.start_time < TOO_OLD:
            Log.warning("Do not try to process old buildbot logs")
            return set()

        try:
            rev = Data(
                changeset={"id": data.build.revision},
                branch={"name": data.build.branch, "locale": data.build.locale}
            )
            data.repo = minimize_repo(resources.hg.get_revision(rev))
            data.build.date = coalesce(data.build.date, data.repo.changeset.date)
        except Exception as e:
            if data.action.start_time > Date.today()-MONTH:
                # ONLY SEND WARNING IF IT IS RECENT
                send = Log.warning
            else:
                send = Log.note

            send(
                "Can not get revision for branch={{branch}}, locale={{locale}}, revision={{revision}}\n{{details|json|indent}}",
                branch=data.build.branch,
                locale=data.build.locale,
                revision=data.build.revision,
                details=data,
                cause=e
            )

        url = data.run.logurl
        data.etl = set_default(
            {
                "file": url,
                "timestamp": Date.now().unix,
                "revision": git.get_revision(),
            },
            buildbot_data.etl
        )

        with Timer("Read {{url}}", {"url": url}, silent=not DEBUG) as timer:
            try:
                if url == None:
                    data.etl.error = "No logurl"
                    output.append(data)
                    continue

                if "scl3.mozilla.com" in url:
                    Log.alert("Will not read {{url}}", url=url)
                    data.etl.error = "Text log unreachable"
                    output.append(data)
                    continue

                response = http.get(
                    url=[
                        url,
                        url.replace("http://ftp.mozilla.org", "http://archive.mozilla.org"),
                        url.replace("http://ftp.mozilla.org", "http://ftp-origin-scl3.mozilla.org")
                    ],
                    retry={"times": 3, "sleep": 10}
                )
                if response.status_code == 404:
                    Log.note("Text log does not exist {{url}}", url=url)
                    data.etl.error = "Text log does not exist"
                else:
                    all_log_lines = response.get_all_lines(encoding=None)
                    action = process_text_log(all_log_lines, url, source_key)
                    set_default(data.action, action)

                data.action.duration = data.action.end_time - data.action.start_time

                verify_equal(data, "build.revision", "action.revision", url, from_key=source_key)
                verify_equal(data, "build.id", "action.buildid", url, from_key=source_key)
                verify_equal(data, "run.key", "action.builder", warning=False, from_url=url, from_key=source_key)
                verify_equal(data, "run.machine.name", "action.subordinate", from_url=url, from_key=source_key)

                output.append(elasticsearch.scrub(data))
                Log.note("Found builder record for id={{id}}", id=etl2key(data.etl))
            except Exception as e:
                e = Except.wrap(e)  # SO `in` OPERATOR WORKS
                if "Problem with calculating durations" in e:
                    Log.error("Prioritized error", cause=e)
                elif "Connection reset by peer" in e:
                    Log.error(TRY_AGAIN_LATER, reason="connection problem", cause=e)
                elif "incorrect header check" in e:
                    Log.error(TRY_AGAIN_LATER, reason="connection problem", cause=e)
                elif "An existing connection was forcibly closed by the remote host" in e:
                    Log.error(TRY_AGAIN_LATER, reason="connection problem", cause=e)

                Log.warning("Problem processing {{key}}: {{url}}", key=source_key, url=url, cause=e)
                data.etl.error = "Text log unreadable"
                output.append(data)

        data.etl.duration = timer.duration

    dest_bucket.extend([{"id": etl2key(d.etl), "value": d} for d in output])
    return {source_key}

