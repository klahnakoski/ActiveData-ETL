# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Trung Do (chin.bimbo@gmail.com)
#
from __future__ import division
from __future__ import unicode_literals

import re
from copy import copy

import mo_math

from activedata_etl.imports import buildbot
from jx_python import jx
from mo_dots import wrap, FlatList, Null, Data, unwrap, set_default
from mo_future import text
from mo_json import json2value
from mo_logs import Log, strings
from mo_logs.exceptions import Except
from mo_math import MAX, MIN
from mo_times.dates import Date, unicode2Date
from mo_times.durations import SECOND, MINUTE, HOUR, DAY
from pyLibrary.convert import quote2string

DEBUG = False
MAX_TIMING_ERROR = SECOND  # SOME TIMESTAMPS ARE ONLY ACCURATE TO ONE SECOND
MAX_HARNESS_TIMING_ERROR = 5 * MINUTE


def process_tc_live_backing_log(source_key, all_log_lines, from_url, task_record):
    """
        [taskcluster 2016-10-04 17:09:02.626Z] Task ID: abzkq-CjS_KJzNEhE6nVhA
        [taskcluster 2016-10-04 17:09:02.626Z] Worker ID: i-0348d7e9408f77f42
        [taskcluster 2016-10-04 17:09:02.626Z] Worker Group: us-east-1c
        [taskcluster 2016-10-04 17:09:02.626Z] Worker Node Type: m3.xlarge
        [taskcluster 2016-10-04 17:09:02.626Z] Worker Type: desktop-test-xlarge
        [taskcluster 2016-10-04 17:09:02.626Z] Public IP: 54.167.236.92
        [taskcluster 2016-10-04 17:09:02.626Z] using cache "level-3-checkouts" -> /home/worker/checkouts
        [taskcluster 2016-10-04 17:09:02.626Z] using cache "tooltool-cache" -> /home/worker/tooltool-cache
        [taskcluster 2016-10-04 17:09:02.626Z] using cache "level-3-autoland-test-workspace" -> /home/worker/workspace
        [taskcluster 2016-10-04 17:09:02.626Z] using cache "level-3-hg-shared" -> /home/worker/hg-shared

        [taskcluster 2016-10-04 17:09:03.469Z] Image 'public/image.tar' from task 'eVRm3dsTRX2QKogGKblqsQ' loaded.  Using image ID sha256:a7d7a120fc020ff9042b7a29ddc750bc8033f8f14481a4031f66281e4eaf9e55.
        [taskcluster 2016-10-04 17:09:03.554Z] === Task Starting ===
        [setup 2016-10-04T17:09:03.770657Z] run-task started
        [chown 2016-10-04T17:09:03.773065Z] changing ownership of /home/worker/workspace to worker:worker
        [setup 2016-10-04T17:09:03.773149Z] running as worker:worker
        [task 2016-10-04T17:09:03.773242Z] executing ['/home/worker/bin/test-linux.sh', '--no-read-buildbot-config', '--installer-url=https://queue.taskcluster.net/v1/task/NWtLXuuETQ-lkYu4TdyRqw/artifacts/public/build/target.apk', '--test-packages-url=https://queue.taskcluster.net/v1/task/NWtLXuuETQ-lkYu4TdyRqw/artifacts/public/build/target.test_packages.json', '--test-suite=jsreftest', '--total-chunk=6', '--this-chunk=5', '--download-symbols=ondemand']
        [task 2016-10-04T17:09:03.776154Z] + set -x -e
        [task 2016-10-04T17:09:03.776602Z] ++ id
        [task 2016-10-04T17:09:03.778726Z] + echo 'running as' 'uid=1000(worker)' 'gid=1000(worker)' 'groups=1000(worker),44(video)'
        [task 2016-10-04T17:09:03.778800Z] running as uid=1000(worker) gid=1000(worker) groups=1000(worker),44(video)
    """

    process_head = True
    old_head = False
    new_head = False
    accumulate_head = []
    action = Data()
    action.timings = []

    start_time = task_record.task.run.start_time
    end_time = task_record.task.run.end_time

    harness_steps = {}
    task_steps = Data()

    new_mozharness_line = NewHarnessLines()

    total_bytes = 0

    for log_ascii in all_log_lines:
        total_bytes += len(log_ascii) + 1

        if not log_ascii.strip():
            continue

        try:
            log_line = log_ascii.decode('utf8', "ignore").strip()
        except Exception as e:
            if not DEBUG:
                Log.warning("Really bad log line ignored while processing {{url}}\n{{line}}", url=from_url, line=log_ascii, cause=e)
            continue

        try:
            prefix = strings.between(log_line, "[", "]")
            if prefix and log_line.startswith("[" + prefix):
                prefix_words = prefix.split(' ')
                tc_timestamp = Date(' '.join(prefix_words[1:]))
                step_name = prefix_words[0]
                curr_line = log_line[len(prefix) + 3:]

                start_time = MIN([start_time, tc_timestamp])
                end_time = MAX([end_time, tc_timestamp])

                task_step = task_steps[step_name]
                if not task_step:
                    task_step = task_steps[step_name] = Data()
                    task_step.step = step_name
                    action.timings.append(task_step)
                task_step.start_time = mo_math.min(task_step.start_time, tc_timestamp)
                task_step.end_time = MAX([task_step.end_time, tc_timestamp])
            else:
                # OLD, NON-PREFIXED, FORMAT IS LEGITIMATE
                process_head = False
                curr_line = log_line
        except Exception as _:
            # OLD, NON-PREFIXED, FORMAT IS LEGITIMATE
            process_head = False
            curr_line = log_line

        if process_head:
            # [taskcluster 2016-10-04 17:09:02.626Z] Task ID: abzkq-CjS_KJzNEhE6nVhA
            # [taskcluster 2016-10-04 17:09:02.626Z] Worker ID: i-0348d7e9408f77f42
            # [taskcluster 2016-10-04 17:09:02.626Z] Worker Group: us-east-1c
            # [taskcluster 2016-10-04 17:09:02.626Z] Worker Node Type: m3.xlarge
            # [taskcluster 2016-10-04 17:09:02.626Z] Worker Type: desktop-test-xlarge
            # [taskcluster 2016-10-04 17:09:02.626Z] Public IP: 54.167.236.92
            # [taskcluster 2016-10-04 17:09:02.626Z] using cache "level-3-checkouts" -> /home/worker/checkouts
            # [taskcluster 2016-10-04 17:09:02.626Z] using cache "tooltool-cache" -> /home/worker/tooltool-cache
            # [taskcluster 2016-10-04 17:09:02.626Z] using cache "level-3-autoland-test-workspace" -> /home/worker/workspace
            # [taskcluster 2016-10-04 17:09:02.626Z] using cache "level-3-hg-shared" -> /home/worker/hg-shared


            # [taskcluster 2018-03-07T06:28:51.049Z] Worker Type (gecko-t-win10-64) settings:
            # [taskcluster 2018-03-07T06:28:51.050Z]   {
            # [taskcluster 2018-03-07T06:28:51.050Z]     "aws": {
            # [taskcluster 2018-03-07T06:28:51.050Z]       "ami-id": "ami-7a131a1a",
            # [taskcluster 2018-03-07T06:28:51.050Z]       "availability-zone": "us-west-1b",
            # [taskcluster 2018-03-07T06:28:51.050Z]       "instance-id": "i-0b489ebae9ce39eab",
            # [taskcluster 2018-03-07T06:28:51.050Z]       "instance-type": "c4.2xlarge",
            # [taskcluster 2018-03-07T06:28:51.050Z]       "local-ipv4": "10.143.16.108",
            # [taskcluster 2018-03-07T06:28:51.050Z]       "public-hostname": "ec2-18-144-3-174.us-west-1.compute.amazonaws.com",
            # [taskcluster 2018-03-07T06:28:51.050Z]       "public-ipv4": "18.144.3.174"
            # [taskcluster 2018-03-07T06:28:51.051Z]     },
            # [taskcluster 2018-03-07T06:28:51.051Z]     "config": {
            # [taskcluster 2018-03-07T06:28:51.051Z]       "deploymentId": "137c8c1b0e4b",
            # [taskcluster 2018-03-07T06:28:51.051Z]       "runTaskAsCurrentUser": true
            # [taskcluster 2018-03-07T06:28:51.051Z]     },
            # [taskcluster 2018-03-07T06:28:51.051Z]     "generic-worker": {
            # [taskcluster 2018-03-07T06:28:51.051Z]       "go-arch": "amd64",
            # [taskcluster 2018-03-07T06:28:51.051Z]       "go-os": "windows",
            # [taskcluster 2018-03-07T06:28:51.051Z]       "go-version": "go1.7.5",
            # [taskcluster 2018-03-07T06:28:51.051Z]       "release": "https://github.com/taskcluster/generic-worker/releases/tag/v8.3.0",
            # [taskcluster 2018-03-07T06:28:51.051Z]       "version": "8.3.0"
            # [taskcluster 2018-03-07T06:28:51.051Z]     },
            # [taskcluster 2018-03-07T06:28:51.052Z]     "machine-setup": {
            # [taskcluster 2018-03-07T06:28:51.052Z]       "ami-created": "2018-02-14 15:05:41.795Z",
            # [taskcluster 2018-03-07T06:28:51.052Z]       "manifest": "https://github.com/mozilla-releng/OpenCloudConfig/blob/137c8c1b0e4b3927f15cf38ee4f9771894818221/userdata/Manifest/gecko-t-win10-64.json"
            # [taskcluster 2018-03-07T06:28:51.052Z]     }
            # [taskcluster 2018-03-07T06:28:51.052Z]   }
            # [taskcluster 2018-05-14T19:35:39.375Z] Task ID: Yl5tnpaBQTGjduaj76r2PQ
            # [taskcluster 2018-03-07T06:28:51.052Z] === Task Starting ===
            #
            if curr_line.startswith("=== Task Starting ==="):
                process_head = False
                try:
                    if new_head or not old_head:
                        if accumulate_head[-1].startswith("Task ID:"):
                            accumulate_head = accumulate_head[:-1]
                        if accumulate_head[0].endswith(" settings:"):
                            accumulate_head = accumulate_head[1:]
                        set_default(task_record, {"run": json2value("".join(accumulate_head))})
                        accumulate_head = None
                        continue
                except Exception as e:
                    Log.warning("Expecting JSON header at url={{url}}", url=from_url, cause=e)
            else:
                accumulate_head.append(curr_line)
            try:
                key, value = map(strings.trim, curr_line.split(": "))
                if key.startswith('"'):
                    # LOOKS LIKE JSON
                    new_head = True
                    pass
                elif key == "Task ID":
                    old_head = True
                    if value != task_record.task.id:
                        Log.error("Task in log not matching task details")
                elif key == "Worker Node Type":
                    old_head = True
                    task_record.run.machine.aws.instance_type = value
                elif key == "Worker Type":
                    old_head = True
                    task_record.run.machine.tc_worker_type = value
            except Exception as e:
                pass

            continue

        mozharness_says = new_mozharness_line.match(from_url, end_time, curr_line)
        if mozharness_says:
            timestamp, mode, result, harness_step_name = mozharness_says

            step_name = "mozharness"
            task_step = task_steps[step_name]
            if not task_step:
                task_step = task_steps[step_name] = Data()
                task_step.step = step_name
                action.timings.append(task_step)

            start_time = MIN([start_time, timestamp])
            end_time = MAX([end_time, timestamp])
            task_step.start_time = mo_math.min(task_step.start_time, timestamp)
            task_step.end_time = MAX([task_step.end_time, timestamp])

            harness_step = harness_steps.get(harness_step_name)
            if not harness_step:
                harness_step = harness_steps[harness_step_name] = {
                    "step": harness_step_name,
                    "mode": mode,
                    "start_time": timestamp
                }
                task_step.children += [harness_step]
            else:
                harness_step['result'] = result
                harness_step['end_time'] = timestamp

    try:
        fix_overlap(action.timings)
        fix_times(action.timings, start_time, end_time, source_key)

        new_build_times = FlatList()
        # GO IN REVERSE SO WE CAN INSERT INTO THE LIST
        for b in action.timings:
            new_build_times.append(b)
            if not b.children:
                continue

            fix_times(b.children, b.start_time, b.end_time, source_key)
            # INJECT CHILDREN INTO THIS LIST
            new_build_times.extend([
                {
                    "step": b.step,
                    "harness": c
                }
                for c in b.children
            ])
            b.children = None

        for i, t in enumerate(new_build_times):
            t.order = i
        action.timings = new_build_times

    except Exception as e:
        Log.error("Problem with calculating durations from {{url}}", url=from_url, cause=e)

    action.start_time = start_time
    action.end_time = end_time
    action.duration = end_time - start_time
    # action.harness_time_zone = new_mozharness_line.time_zone
    # action.harness_time_skew = new_mozharness_line.time_skew

    action.etl.total_bytes = total_bytes
    return action


def process_text_log(all_log_lines, from_url, source_key):
    """
    Buildbot logs:

        builder: ...
        subordinate: ...
        starttime: ...
        results: ...
        buildid: ...
        builduid: ...
        revision: ...

        ======= <step START marker> =======
    """

    process_head = True
    data = Data()
    harness_steps = {}
    data.timings = []

    start_time = None
    end_time = None
    builder_raw_step_name = None
    builder_line = BuilderLines()
    old_mozharness_line = OldHarnessLines()
    new_mozharness_line = NewHarnessLines()

    prev_line = ""
    curr_line = ""
    next_line = ""

    total_bytes = 0

    for log_ascii in all_log_lines:
        total_bytes += len(log_ascii) + 1

        if not log_ascii.strip():
            continue

        try:
            log_line = log_ascii.decode('utf8', "ignore").strip()
        except Exception as e:
            if not DEBUG:
                Log.warning("Bad log line ignored while processing {{url}}\n{{line}}", url=from_url, line=log_ascii, cause=e)
            continue

        prev_line = curr_line
        curr_line = next_line
        next_line = log_line

        if not curr_line:
            continue

        if process_head:
            # builder: mozilla-inbound_ubuntu32_vm_test-mochitest-e10s-browser-chrome-3
            # subordinate: tst-linux32-spot-149
            # starttime: 1443701997.36
            # results: success (0)
            # buildid: 20151001042128
            # builduid: 64d75a07877a458fb9f21220ae4cb5a8
            # revision: e23e76de2669b437c2f2576614c9936c713906f4
            try:
                if any(curr_line.startswith(h) for h in BAD_HEADERS):  # COMMON PATTERN
                    process_head = False
                    data["mozconfig_load_error"] = True
                    continue

                key, value = curr_line.split(": ")
                if key == "starttime":
                    data["start_time"] = start_time = end_time = Date(float(value))
                    if DEBUG:
                        Log.note("start_time = {{start_time|date}}", start_time=start_time)
                elif key == "results":
                    data["buildbot_status"] = buildbot.STATUS_CODES[value]
                else:
                    data[key] = value
                continue
            except Exception as e:
                e = Except.wrap(e)
                builder_says = builder_line.match(start_time, curr_line, next_line)
                if not builder_says:
                    Log.warning("Log header {{log_line|quote}} can not be processed (url={{url}})", log_line=curr_line, url=from_url, cause=e)
                    continue
        else:
            builder_says = builder_line.match(start_time, curr_line, next_line)

        if builder_says:
            process_head = False
            timestamp, elapsed, builder_raw_step_name, command, parts, done, status = builder_says

            end_time = MAX([end_time, timestamp])

            if done:
                if builder_step.raw_step == builder_raw_step_name:
                    builder_step.end_time = timestamp
                    builder_step.status = status
                    builder_step.elapsedTime = elapsed
                else:
                    builder_step = wrap({
                        "raw_step": builder_raw_step_name,
                        "step": command,
                        "parts": parts,
                        "start_time": builder_step.end_time,
                        "end_time": timestamp,
                        "elapsedTime": elapsed,
                        "status": status
                    })
                    data.timings.append({"builder": builder_step})
            else:
                builder_step = wrap({
                    "raw_step": builder_raw_step_name,
                    "step": command,
                    "parts": parts,
                    "start_time": timestamp,
                    "status": status
                })
                data.timings.append({"builder": builder_step})
            continue


        mozharness_says = new_mozharness_line.match(from_url, end_time, curr_line)
        if mozharness_says:
            timestamp, mode, result, harness_step_name = mozharness_says
            end_time = MAX([end_time, timestamp])

            if not result:
                harness_step = harness_steps[harness_step_name] = {
                    "step": harness_step_name,
                    "mode": mode,
                    "start_time": timestamp
                }
                builder_step.children += [harness_step]
            else:
                harness_step = harness_steps[harness_step_name]
                harness_step['result'] = result
                harness_step['end_time'] = timestamp

        mozharness_says = old_mozharness_line.match(from_url, end_time, prev_line, curr_line, next_line)
        if mozharness_says:
            timestamp, mode, harness_step_name = mozharness_says
            end_time = MAX([end_time, timestamp])

            builder_step.children += [{
                "step": harness_step_name,
                "mode": mode,
                "start_time": timestamp
            }]

    try:
        fix_times(data.timings.builder, start_time, end_time, source_key)
        new_build_times = FlatList()
        # GO IN REVERSE SO WE CAN INSERT INTO THE LIST
        for b in data.timings:
            new_build_times.append(b)
            b = b.builder
            if not b.children:
                continue

            fix_times(b.children, b.start_time, b.end_time, source_key)
            # INJECT CHILDREN INTO THIS LIST
            new_build_times.extend([
                {
                    "builder": {"step": b.step},
                    "harness": c
                }
                for c in b.children
            ])
            b.children = None

        for i, t in enumerate(new_build_times):
            t.order = i
        data.timings = new_build_times

    except Exception as e:
        Log.error("Problem with calculating durations from {{url}}", url=from_url, cause=e)

    data.end_time = end_time
    data.duration = end_time - start_time
    data.builder_time_zone = builder_line.time_zone
    # data.harness_time_zone = coalesce(new_mozharness_line.time_zone, old_mozharness_line.time_zone)
    # data.harness_time_skew = coalesce(new_mozharness_line.time_skew, old_mozharness_line.time_skew)

    data.etl.total_bytes = total_bytes
    return data





NEW_MOZLOG_STEP = re.compile(r"\d\d:\d\d:\d\d     INFO - \[mozharness\: (.*)Z\] .*")
NEW_MOZLOG_START_STEP = re.compile(r"\d\d:\d\d:\d\d     INFO - \[mozharness\: (.*)Z\] (Running|Skipping) (.*) step.")
NEW_MOZLOG_END_STEP = [
    re.compile(r"\d\d:\d\d:\d\d     INFO - \[mozharness\: (.*)Z\] (.*) ()summary:"),
    re.compile(r"\d\d:\d\d:\d\d     INFO - \[mozharness\: (.*)Z\] Finished (.*) step \((.*)\)")  # example: [mozharness: 2016-07-11 21:35:08.292Z] Finished run-tests step (success)
]

# 16:16:40     INFO - [mozharness: 2019-03-29 16:16:40.033128Z] Config File 1: test/test.json
NEW_MOZLOG_IGNORE_STEP = [
    re.compile(r"\d\d:\d\d:\d\d     INFO - \[mozharness\: (.*)Z\] Config File"),
    re.compile(r"\d\d:\d\d:\d\d     INFO - \[mozharness\: (.*)Z\] Not from any config file"),
]


class NewHarnessLines(object):

    def __init__(self):
        # self.time_zone = None
        # self.time_skew = None
        self.last_seen = None

    def match(self, source, last_timestamp, curr_line):
        """
        :param source:  For debugging
        :param last_timestamp: To ensure the timestamps are in order
        :param prev_line: not used
        :param curr_line: A log line
        :param next_line: not used
        :return: (timestamp, mode, message) if found else None

        EXAMPLE
        012345678901234567890123456789012345678901234567890123456789
        [mozharness: 2016-07-11 21:35:08.2927233Z] Finished run-tests step (success)

        12:23:12     INFO - [mozharness: 2016-11-10 20:23:12.172233Z] Finished run-tests step (success)
        """

        if not NEW_MOZLOG_STEP.match(curr_line):
            return None

        match = NEW_MOZLOG_START_STEP.match(curr_line)
        if match:
            _utc_time, mode, message = match.group(1, 2, 3)
            timestamp = self.utc_to_timestamp(_utc_time, last_timestamp)
            mode = mode.strip().lower()
            if DEBUG:
                Log.note("{{line}}", line=curr_line)
            return timestamp, mode, None, message

        for p in NEW_MOZLOG_END_STEP:
            match = p.match(curr_line)
            if match:
                _utc_time, message, result = match.group(1, 2, 3)
                timestamp = self.utc_to_timestamp(_utc_time, last_timestamp)
                result = result.strip().lower()
                return timestamp, None, result, message

        for p in NEW_MOZLOG_IGNORE_STEP:
            match = p.match(curr_line)
            if match:
                break
        else:
            Log.warning("unexpected [mozharness] line in {{source}}\n{{line}}", source=source, line=curr_line)
        return None

    def utc_to_timestamp(self, _utc_time, last_timestamp):
        timestamp = unicode2Date(_utc_time, format="%Y-%m-%d %H:%M:%S.%f")
        # if last_timestamp == None:
        #     last_timestamp = timestamp
        # elif timestamp < last_timestamp - 12 * HOUR - MAX_HARNESS_TIMING_ERROR:
        #     Log.error("not expected")
        # if self.time_zone is None:
        #     self.time_skew = last_timestamp - timestamp
        #     self.time_zone = mo_math.ceiling((self.time_skew - MAX_HARNESS_TIMING_ERROR) / HOUR) * HOUR
        #     if DEBUG:
        #         Log.note("Harness time zone is {{zone}}", zone=self.time_zone / HOUR)
        # timestamp += self.time_zone
        self.last_seen = MAX([timestamp, self.last_seen])
        return timestamp


OLD_MOZLOG_STEP = re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### (Running|Skipping) (.*) step.")
# 17:54:20     INFO - ##### Finished clobber step (success)
OLD_MOZLOG_SUMMARY = [
    re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### (.*) summary:"),
    re.compile(r"(\d\d:\d\d:\d\d)     INFO - ##### Finished (.*) step \(.*\)")
]
OLD_MOZLOG_PREFIX = re.compile(r"\d\d:\d\d:\d\d     INFO - #####")


class OldHarnessLines(object):

    def __init__(self):
        self.time_zone = None
        self.time_skew = None

    def match(self, source, last_timestamp, prev_line, curr_line, next_line):
        """
        log_date - IN %Y-%m-%d FORMAT FOR APPENDING TO THE TIME-OF-DAY STAMPS
        FOUND IN LOG LINES

        RETURN (timestamp, mode, message) TRIPLE IF FOUND

        EXAMPLE
        012345678901234567890123456789012345678901234567890123456789
        05:20:05     INFO - #####
        05:20:05     INFO - ##### Running download-and-extract step.
        05:20:05     INFO - #####
        """

        if len(next_line) != 25 or len(prev_line) != 25:
            return None
        if not OLD_MOZLOG_PREFIX.match(next_line) or not OLD_MOZLOG_PREFIX.match(prev_line) or not OLD_MOZLOG_PREFIX.match(curr_line):
            return None
        match = OLD_MOZLOG_STEP.match(curr_line)
        if match:
            _time, mode, message = match.group(1, 2, 3)
            mode = mode.strip().lower()
        else:
            for p in OLD_MOZLOG_SUMMARY:
                match = p.match(curr_line)
                if match:
                    break
            else:
                Log.warning("unexpected log line in {{source}}\n{{line}}", source=source, line=curr_line)
                return None

            # SOME MOZHARNESS STEPS HAVE A SUMMARY, IGNORE THEM
            return None

        timestamp = Date((last_timestamp - 12 * HOUR).format("%Y-%m-%d") + " " + _time, "%Y-%m-%d %H:%M:%S")
        if timestamp < last_timestamp - 12 * HOUR - MAX_HARNESS_TIMING_ERROR:
            timestamp += DAY
        if self.time_zone is None:
            self.time_skew = last_timestamp - timestamp
            self.time_zone = mo_math.ceiling((self.time_skew - MAX_HARNESS_TIMING_ERROR) / HOUR) * HOUR
            if DEBUG:
                Log.note("Harness time zone is {{zone}}", zone=self.time_zone / HOUR)
        timestamp += self.time_zone

        if DEBUG:
            Log.note("{{line}}", line=curr_line)
        return timestamp, mode, message



BUILDER_ELAPSE = re.compile(r"elapsedTime=(\d+\.\d*)")  # EXAMPLE: elapsedTime=2.545

class BuilderLines(object):


    def __init__(self):
        self.time_zone = None
        self.last_elapse_time = None
        self.last_elapse_time_age = 0  # KEEP TRACK OF HOW MANY LINES AGO WE SAW elapsedTime

    def match(self, start_time, line, next_line):
        """
        RETURN (timestamp, elapsed, message, done, status) QUADRUPLE

        THERE IS A LINE, A LITTLE BEFORE "======= Finish..." WHICH HAS elapsedTime
        EXAMPLE
        elapsedTime=2.545


        EXAMPLES
        ========= Started '/tools/buildbot/bin/python scripts/scripts/android_emulator_unittest.py ...' failed (results: 5, elapsed: 1 hrs, 12 mins, 59 secs) (at 2015-10-04 10:46:12.401377) =========
        ========= Started 'c:/mozilla-build/python27/python -u ...' warnings (results: 1, elapsed: 19 mins, 0 secs) (at 2015-10-04 07:52:22.752839) =========
        ========= Started '/tools/buildbot/bin/python scripts/scripts/b2g_emulator_unittest.py ...' interrupted (results: 4, elapsed: 22 mins, 59 secs) (at 2015-10-05 00:51:02.915315) =========
        ========= Started 'rm -f ...' (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131322) =========
        c set props: build_url blobber_files (results: 0, elapsed: 0 secs) (at 2015-10-01 05:30:53.131005) =========
        ========= Skipped  (results: not started, elapsed: not started) =========
        """
        elapse = BUILDER_ELAPSE.match(line)
        if elapse:
            self.last_elapse_time = float(elapse.group(1))
            self.last_elapse_time_age = 0
            return
        else:
            self.last_elapse_time_age += 1

        if self.last_elapse_time_age > 5:
            # TOO LONG AGO, EXPECTING THIS NEAR THE Finish LINE
            last_elapse_time = None

        if not line.startswith("========= ") or not line.endswith(" ========="):
            return None

        try:
            parts = list(map(text.strip, line[10:-10].split("(")))
            if parts[0].startswith("main_lag:"):
                return None
            if parts[0] == "Skipped":
                # NOT THE REGULAR PATTERN
                message, status, parts, timestamp, done = "", "skipped", None, Null, True
                if DEBUG:
                    Log.note("{{line}}", line=line)
                return timestamp, self.last_elapse_time, "", message, parts, done, status
            desc, stats, _time = "(".join(parts[:-2]), parts[-2], parts[-1]

        except Exception as e:
            Log.warning("Can not split log line: {{line|quote}}", line=line, cause=e)
            return None

        if desc.startswith("Started "):
            done = False
            message = desc[8:].strip()
        elif desc.startswith("Finished "):
            done = True
            message = desc[9:].strip()
        else:
            Log.warning("Can not parse log line: {{line}}", line=line)
            return None

        result_code = int(stats.split(",")[0].split(":")[1].strip())
        status = buildbot.STATUS_CODES[result_code]

        if message.endswith(" failed") and status in ["retry", "failure"]:
            #SOME message END WITH "failed" ON RETRY
            message = message[:-7].strip()
        elif message.endswith(" interrupted") and status in ["exception", "retry"]:
            message = message[:-12].strip()
        elif message.endswith(" " + status):
            #SOME message END WITH THE STATUS STRING
            message = message[:-(len(status) + 1)].strip()

        if not done:
            command, parts = parse_builder_message(message, next_line)
        else:
            command = None

        timestamp = Date(_time[3:-1], "%Y-%m-%d %H:%M:%S.%f")
        if self.time_zone is None:
            self.time_zone = mo_math.ceiling((start_time - timestamp - MAX_TIMING_ERROR) / HOUR) * HOUR
            if DEBUG:
                Log.note("Builder time zone is {{zone}}", zone=self.time_zone/HOUR)
        timestamp += self.time_zone

        if DEBUG:
            Log.note("{{line}}", line=line)
        return timestamp, self.last_elapse_time, message, command, parts, done, status


def parse_builder_message(message, next_line):
    if message.startswith("set props: "):
        parts = message.split(" ")[2:]
        message = "set props"
    elif message.startswith("mock-install "):
        # mock-install autoconf213 mozilla-python27 zip mozilla-python27-mercurial git ccache glibc-static libstdc++-static perl-Test-Simple perl-Config-General gtk2-devel libnotify-devel yasm alsa-lib-devel libcurl-devel wireless-tools-devel libX11-devel libXt-devel mesa-libGL-devel gnome-vfs2-devel mpfr xorg-x11-font imake ccache wget gcc472_0moz1 gcc473_0moz1 freetype-2.3.11-6.el6_2.9 freetype-devel-2.3.11-6.el6_2.9 gstreamer-devel gstreamer-plugins-base-devel
        parts = message.split(" ")[1:]
        message = "mock-install"
    elif message.startswith("python "):
        # python c:/builds/moz2_subordinate/rel-m-beta-w32_bld-00000000000/build/build/pymake/make.py partial mar
        message = message.split(" ")[1].split("/")[-1]
        parts = None
    elif message.startswith("'/tools/buildbot/bin/python "):
        # '/tools/buildbot/bin/python scripts/scripts/desktop_unittest.py ...'
        message = message.split(" ")[1].split("/")[-1]
        parts = None
    elif message.startswith("'python "):
        message = message.split(" ")[1].split("/")[-1]
        parts = None
    elif message.startswith("'sh "):
        # 'sh c:/builds/moz2_subordinate/tb-c-esr38-w32-000000000000000/tools/scripts/tooltool/tooltool_wrapper.sh ...'
        message = message.split(" ")[1].split("/")[-1]
        parts = None
    elif message.startswith("'perl "):
        # 'sh c:/builds/moz2_subordinate/tb-c-esr38-w32-000000000000000/tools/scripts/tooltool/tooltool_wrapper.sh ...'
        message = message.split(" ")[1].split("/")[-1]
        parts = None
    elif message.startswith("'c:/mozilla-build/python27/python -u "):
        if not next_line:
            return message, None
        new_message = parse_command_line(next_line)[2].split("/")[-1]
        return parse_builder_message(new_message, "")
    elif message == "'bash -c ...'":
        if not next_line:
            return message, None
        new_message = " ".join(parse_command_line(next_line)[2:])
        return parse_builder_message(new_message, "")
    elif message.startswith("wget "):
        temp = message.split(" ")
        url = wrap([t for t in temp[1:] if not t.startswith("-")])[0]
        file = url.split("/")[-1]
        message = temp[0] + " " + file
        parts = temp[1:]
    else:
        parts = None

    return message, parts


def parse_command_line(line):
    """
    space separated, single-quoted strings
    """
    output = []
    value = ""
    i = 0
    while i < len(line):
        c = line[i]
        i += 1
        if c == " ":
            if value:
                output.append(value)
            value = ""
        elif c == "'":
            value += c
            c = line[i]
            i += 1
            while True:
                if c == "'":
                    value += c
                    output.append(quote2string(value))
                    value = ""
                    break
                elif c == "\\":
                    value += c + line[i]
                    i += 1
                else:
                    value += c

                c = line[i]
                i += 1
        else:
            value += c
    return output


BAD_HEADERS = [
    "New python executable in ",
    "buildid: Error loading mozconfig: ",
    "Traceback (most recent call last):"
]


def fix_overlap(times):
    """
    THE times CAN LEGITIMATELY OVERLAP, CONVERT TO NON-OVERLAPPING STEPS
    AND RE_LABEL AS REQUIRED
    :param times:
    :return:
    """
    while True:
        for ia, a in enumerate(times):
            b = times[ia + 1]
            if a.start_time < b.start_time and b.end_time < a.end_time:
                pre = copy(a)
                post = copy(a);
                pre.step = a.step.split(" (post ")[0] + " (pre " + b.step + ")"
                pre.end_time = b.start_time
                post.step = a.step.split(" (post ")[0] + " (post " + b.step + ")"
                post.start_time = b.end_time
                times[ia] = pre
                for ic, c in enumerate(times[ia + 2::]):
                    if post.start_time <= c.start_time:
                        unwrap(times).insert(ia + ic + 2, post)
                        break
                else:
                    times.append(post)
                break
        else:
            break


def fix_times(times, start_time, end_time, source_key):
    if start_time == None or end_time == None:
        Log.error("Require a time range")
    if not times:
        return

    time = start_time
    for i, t in enumerate(times):
        if t.start_time == None:
            # FIND BEST EVIDENCE OF WHEN THIS STARTED
            t.start_time = MIN([MIN(t.children.start_time), MIN(t.children.end_time), time])
        time = MAX([t.start_time, t.end_time, time])

    # EVERY TIME NOW HAS A start_time
    time = end_time
    for t in jx.reverse(times):
        if t.end_time == None:
            # FIND BEST EVIDENCE OF WHEN THIS ENDED (LOTS OF CANCELLED JOBS)
            t.end_time = MAX([MAX(t.children.start_time), MAX(t.children.end_time), time, t.start_time])
        t.duration = MAX([time, t.end_time]) - t.start_time
        if t.duration < 0 and end_time.floor(DAY).unix == 1478390400: # 6 nov 2016
            t.duration+=HOUR
        if t.duration == None or t.duration < 0:
            if t.duration > -1 and t.raw_step == u'downloading to buildprops.json':
                t.duration = 0
                Log.warning("negative time detected in step {{step|quote}} for key {{key}}, but small enough to ignore", key=source_key, step=t.step)
            else:
                Log.error("logic error")
        time = t.start_time
