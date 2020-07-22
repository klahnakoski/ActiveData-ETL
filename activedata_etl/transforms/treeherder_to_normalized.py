# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import mo_math

from activedata_etl import etl2key, key2etl
from activedata_etl.transforms import TRY_AGAIN_LATER
from mo_dots import Data, listwrap, wrap, set_default, is_data
from mo_hg.hg_mozilla_org import minimize_repo
from mo_json import json2value
from mo_logs import Log, machine_metadata, strings, Except
from mo_times.dates import Date
from jx_elasticsearch import elasticsearch
from pyLibrary.env import git

DEBUG = True
DISABLE_LOG_PARSING = False
MAX_THREADS = 5

seen_tasks = {}
new_seen_tc_properties = set()


def process(source_key, source, destination, resources, please_stop=None):
    output = []
    lines = list(enumerate(source.read_lines()))
    for line_number, line in lines:
        if please_stop:
            Log.error("Shutdown detected. Stopping early")
        try:
            raw_treeherder = json2value(line)
            etl_source = consume(raw_treeherder, "etl")
            if etl_source.source.source.id <= 687:
                etl_source = wrap({"source": key2etl(source_key)})
            etl_source.type = "join"
            etl_source.source.type = "join"

            normalized = Data()
            normalized.etl = set_default(
                {
                    "id": line_number,
                    "timestamp": Date.now(),
                    "machine": machine_metadata,
                    "revision": git.get_revision(),
                    "type": "join",
                },
                etl_source,
            )
            normalize(source_key, resources, raw_treeherder, normalized)
            normalized = elasticsearch.scrub(normalized)
            output.append(normalized)
        except Exception as e:
            e = Except.wrap(e)
            if TRY_AGAIN_LATER in e:
                raise e
            Log.warning(
                "Treeherder line for key {{key}} not processed: {{line|quote}}",
                key=source_key,
                line=line,
                cause=e,
            )

    keys = destination.extend({"id": etl2key(t.etl), "value": t} for t in output)
    return keys


def normalize(source_key, resources, raw_treeherder, new_treeherder):
    raw_job = raw_treeherder.job

    new_treeherder.job.type.name = consume(raw_job, "job_type.name")
    new_treeherder.job.type.description = consume(raw_job, "job_type.description")
    new_treeherder.job.type.symbol = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "job_type.symbol"),
        consume(raw_job, "signature.job_type_symbol"),
    )
    new_treeherder.job.type.name = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "job_type.name"),
        consume(raw_job, "signature.job_type_name"),
    )
    new_treeherder.job.type.group.symbol = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "job_type.job_group.symbol"),
        consume(raw_job, "signature.job_group_symbol"),
        consume(raw_job, "job_group.symbol"),
    )

    new_treeherder.job.type.group.name = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "job_type.job_group.name"),
        consume(raw_job, "job_type.job_group"),
        consume(raw_job, "signature.job_group_name"),
        consume(raw_job, "job_group.name"),
        consume(raw_job, "job_group"),
    )

    new_treeherder.job.guid = consume(raw_job, "guid")
    new_treeherder.job.id = consume(raw_job, "id")
    new_treeherder.job.coalesced_to_guid = consume(raw_job, "coalesced_to_guid")

    # BUILD
    new_treeherder.build.branch = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "repository"),
        consume(raw_job, "signature.repository"),
    )
    new_treeherder.build.revision = consume(raw_job, "push.revision")
    new_treeherder.build.revision12 = new_treeherder.build.revision[0:12]
    new_treeherder.build.date = consume(raw_job, "push.time")

    new_treeherder.build.platform = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "build_platform.platform"),
        consume(raw_job, "signature.build_platform"),
    )
    new_treeherder.build.os = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "build_platform.os_name"),
        consume(raw_job, "signature.build_os_name"),
    )
    new_treeherder.build.architecture = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "build_platform.architecture"),
        consume(raw_job, "signature.build_architecture"),
    )
    new_treeherder.build.product = consume(raw_job, "product.name")

    new_treeherder.run.key = consume(raw_job, "signature.name")
    new_treeherder.run.reason = consume(raw_job, "reason")
    new_treeherder.run.tier = consume(raw_job, "tier")
    new_treeherder.run.result = consume(raw_job, "result")
    new_treeherder.run.state = consume(raw_job, "state")
    if raw_treeherder.job.signature.build_system_type in [
        "taskcluster",
        "buildbot",
        "fx-test-jenkins",
        "fx-test-jenkins-dev",
        "qa-main",
        "qa-preprod-main",
    ]:
        consume(raw_job, "signature.build_system_type")
    else:
        Log.error(
            "Know nothing about build_system_type=={{type}}",
            type=raw_treeherder.job.signature.build_system_type,
        )

    # RUN MACHINE
    new_treeherder.run.machine.name = machine_name = consume(raw_job, "machine.name")
    split_name = machine_name.split("-")
    if mo_math.is_integer(split_name[-1]):
        new_treeherder.run.machine.pool = "-".join(split_name[:-1])
    new_treeherder.run.machine.os = consume(raw_job, "signature.machine_os_name")
    new_treeherder.run.machine.architecture = consume(
        raw_job, "signature.machine_architecture"
    )
    new_treeherder.run.machine.platform = coalesce_w_conflict_detection(
        source_key,
        consume(raw_job, "machine_platform"),
        consume(raw_job, "signature.machine_platform"),
    )

    # ACTION
    new_treeherder.action.start_time = consume(raw_job, "start_time")
    new_treeherder.action.end_time = consume(raw_job, "end_time")
    new_treeherder.action.request_time = consume(raw_job, "submit_time")
    new_treeherder.action.duration = (
        new_treeherder.action.end_time - new_treeherder.action.start_time
    )
    new_treeherder.last_modified = consume(raw_job, "last_modified")

    new_treeherder.failure.auto_classification = consume(raw_job, "autoclassify_status")
    new_treeherder.failure.classification = consume(raw_job, "failure_classification")
    new_treeherder.failure.notes = consume(raw_job, "job_note")

    new_treeherder.repo = {
        "branch": {"name": new_treeherder.build.branch},
        "changeset": {"id": new_treeherder.build.revision},
    }
    try:
        if new_treeherder.build.branch not in NON_HG_BRANCHES:
            new_treeherder.repo = minimize_repo(
                resources.hg.get_revision(new_treeherder.repo)
            )
    except Exception as e:
        Log.warning(
            "Problem with getting info changeset {{changeset}}",
            changeset=new_treeherder.repo,
            cause=e,
        )

    new_treeherder.bugs = consume(raw_job, "bug_job_map")

    pull_job_log(source_key, consume(raw_treeherder.job, "job_log"), new_treeherder)

    consume(raw_job, "push")
    consume(raw_job, "running_eta")
    consume(raw_job, "who")
    consume(raw_job, "signature.first_submission_timestamp")
    consume(raw_job, "signature.option_collection_hash")
    consume(raw_job, "signature.signature")
    pull_details(source_key, consume(raw_treeherder.job, "job_detail"), new_treeherder)

    new_treeherder.run.taskcluster.id = coalesce_w_conflict_detection(
        source_key,
        new_treeherder.run.taskcluster.id,
        consume(raw_job, "taskcluster_metadata.task_id"),
    )
    new_treeherder.run.taskcluster.retry_id = consume(
        raw_job, "taskcluster_metadata.retry_id"
    )

    pull_options(source_key, raw_treeherder, new_treeherder)

    remainder = raw_treeherder.leaves()
    if remainder:
        Log.error(
            "Did not process {{paths}} for key={{key}}",
            key=source_key,
            paths=[k for k, _ in remainder],
        )


def pull_options(source_key, raw_treeherder, new_treeherder):
    options = listwrap(consume(raw_treeherder, "job.option_collection.option"))
    for o in options:
        if o in _option_map:
            new_treeherder.build.type += _option_map[o]
        elif o in ["e10s"]:
            new_treeherder.run.type += [o]
        else:
            Log.warning(
                "not known option {{option|quote}} while processing {{key}}",
                key=source_key,
                option=o,
            )


_option_map = {
    "addon": ["addon"],
    "aarch64-debug": ["aarch64", "debug"],
    "aarch64": ["aarch64"],
    "arm-debug": ["arm", "debug"],
    "ARMv7": ["arm"],
    "arm64": ["arm64"],
    "all": ["all"],
    "asan": ["asan"],
    "ccov": ["ccov"],
    "cc": ["ccov"],
    "debug": ["debug"],
    "fips": ["fips"],
    "fuzz": ["fuzz"],
    "gyp": ["gyp"],
    "gyp-asan": ["gyp", "asan"],
    "jsdcov": ["jsdcov"],
    "lto": ["lto"],  # LINK TIME OPTIMIZATION
    "make": ["make"],
    "nightly": ["nightly"],
    "Nightlies": ["nightly"],
    "nostylo": ["stylo-disabled"],
    "opt": ["opt"],
    "pgo": ["pgo"],
    "tsan": ["tsan"],
    "x64": ["x64"],
    "x86": ["x86"],
    "release": ["release"],
}


def pull_job_log(source_key, job_log, new_treeherder):
    for d in listwrap(job_log):
        for f in listwrap(d.failure_line):
            f.message = None
            f.job_guid = None
            f.stackwalk_stdout = None
            f.stackwalk_stderr = None
            f.subtest = None
    new_treeherder.job_log = job_log


def pull_details(source_key, details, new_treeherder):
    for d in listwrap(details):
        if d.title == "Summary":
            new_treeherder.summary = d.value
        elif d.title == "buildbot_request_id":
            new_treeherder.run.buildbot.id = d.value
        elif d.title == "Inspect Task":
            ci_tc = "https://firefox-ci-tc.services.mozilla.com/tasks/"
            if d.url.startswith(ci_tc):
                new_treeherder.run.taskcluster.id = d.url[len(ci_tc):]
            elif d.url.startswith("https://tools.taskcluster.net/task-inspector/#"):
                new_treeherder.run.taskcluster.id = strings.between(
                    d.url,
                    "https://tools.taskcluster.net/task-inspector/#",
                    "/"
                )
            else:
                Log.warning(
                    "Can not extract task for key {{key}} from {{url}}",
                    key=source_key,
                    url=d.url,
                )
        elif d.title == "CPU idle":
            new_treeherder.stats.cpu_idle = float(
                d.value.split("(")[0].replace(",", "")
            )
        elif d.title == "CPU user":
            new_treeherder.stats.cpu_user = float(
                d.value.split("(")[0].replace(",", "")
            )
        elif d.title == "CPU system":
            new_treeherder.stats.cpu_system = float(
                d.value.split("(")[0].replace(",", "")
            )
        elif d.title == "CPU iowait":
            new_treeherder.stats.cpu_io_wait = float(
                d.value.split("(")[0].replace(",", "")
            )
        elif d.title == "CPU usage":
            new_treeherder.stats.cpu_usage = float(d.value.strip("%").replace(",", ""))
        elif d.title == "I/O read bytes / time":
            new_treeherder.stats.io_read_bytes = float(
                d.value.split("/")[0].strip().replace(",", "")
            )
            new_treeherder.stats.io_read_time = float(
                d.value.split("/")[1].strip().replace(",", "")
            )
        elif d.title == "I/O write bytes / time":
            new_treeherder.stats.io_write_bytes = float(
                d.value.split("/")[0].strip().replace(",", "")
            )
            new_treeherder.stats.io_write_time = float(
                d.value.split("/")[1].strip().replace(",", "")
            )
        elif d.title == "Swap in / out":
            new_treeherder.stats.swap_in = float(
                d.value.split("/")[0].strip().replace(",", "")
            )
            new_treeherder.stats.swap_out = float(
                d.value.split("/")[1].strip().replace(",", "")
            )
        elif d.title in ["artifact uploaded", "One Click Loaner"]:
            pass
        elif d.title.find("-chunked"):
            pass
        elif d.title == None:
            if d.value in [
                "auto clobber",
                "purged clobber",
                "periodic clobber",
                "forced clobber",
            ]:
                pass
            elif any(map(d.value.startswith, ["linker max vsize: ", "num_ctors: "])):
                new_treeherder.stats.linker_max_vsize = int(
                    d.value.split(":")[1].strip()
                )
            elif any(map(d.value.startswith, KNOWN_VALUES)):
                pass
            elif d.value in d.url:
                pass
            else:
                KNOWN_VALUES.append(d.value)
                Log.warning(
                    "value has no title {{value|quote}} while processing {{key}}",
                    key=source_key,
                    value=d.value,
                )
        else:
            Log.warning("can not process detail with title of {{title}}", title=d.title)
    new_treeherder.job.details = details


def coalesce_w_conflict_detection(source_key, *args):
    output = None
    for a in args:
        if a == None:
            continue
        if is_data(a) and not a:
            continue
        if output == None:
            output = a
        elif a != output:
            Log.warning(
                "tried to coalesce {{values_|json}} while processing {{key}}",
                key=source_key,
                values_=args,
            )
        else:
            pass
    return output


def consume(props, key):
    output = props[key]
    try:
        props[key] = None
    except Exception:
        pass
    return output


KNOWN_VALUES = [
    "marionette: ",
    "--",
    "The following arguments ",
    "Tests will be run from the following files:",
    "gaia_revlink: ",
    "Unknown: ",
    "\t--this-chunk=1 --total-chunks=1 -- ",
    "\t--setenv=MOZ_BROWSER_XHTML=",
    "\t--setenv=MOZ_QUIET=",
    "\t--setenv=MOZ_DISABLE_CONTENT_SANDBOX=",
    "\t--setenv=MOZ_LOG="
]


NON_HG_BRANCHES = [
    "android-components",
    'ci-taskgraph',
    "bmo-main",
    "fenix",
    'fxapom',
    "go-bouncer",
    "mozillians-tests",
    'reference-browser',
    "servo-auto",
    "servo-main",
    'servo-prs',
    "servo-try",
    "snippets-service",
    "snippets-service",
    "snippets-tests",
    "socorro",
    "stubattribution-tests",
]
