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
import requests

from activedata_etl import etl2key
from activedata_etl.imports.resource_usage import normalize_resource_usage
from activedata_etl.imports.task import decode_metatdata_name
from activedata_etl.imports.text_log import process_tc_live_backing_log
from activedata_etl.transforms import (
    TRY_AGAIN_LATER,
    TC_ARTIFACT_URL,
    TC_ARTIFACTS_URL,
    TC_STATUS_URL,
    TC_RETRY,
    TC_MAIN_URL,
)
from jx_python import jx
from mo_dots import set_default, Data, unwraplist, listwrap, wrap, coalesce, Null, is_data
from mo_files import URL, mimetype
from mo_future import text
from mo_hg.hg_mozilla_org import minimize_repo
from mo_json import json2value, value2json
from mo_logs import Log, machine_metadata, strings
from mo_logs.exceptions import suppress_exception, Except
from mo_testing.fuzzytestcase import assertAlmostEqual
from mo_times.dates import Date
from pyLibrary import convert
from mo_http import http

DEBUG = False
DISABLE_LOG_PARSING = False
MAX_THREADS = 5

seen_tasks = {}
new_seen_tc_properties = set()


def process(source_key, source, destination, resources, please_stop=None):
    output = []
    source_etl = None

    lines = list(enumerate(source.read_lines()))
    session = requests.session()
    for line_number, line in lines:
        if please_stop:
            Log.error("Shutdown detected. Stopping early")
        try:
            tc_message = json2value(line)
            task_id = consume(tc_message, "status.taskId")  # context.taskId
            etl = consume(tc_message, "etl")
            consume(tc_message, "_meta")

            Log.note(
                "{{id}} found (line #{{num}}) for {{key}}",
                key=source_key,
                id=task_id,
                num=line_number,
                artifact=tc_message.artifact.name,
            )
            task_url = strings.expand_template(TC_MAIN_URL, {"task_id": task_id})
            task = http.get_json(task_url, retry=TC_RETRY, session=session)
            if task.code == "ResourceNotFound":
                Log.note(
                    "Can not find task {{task}} while processing key {{key}}",
                    key=source_key,
                    task=task_id,
                )
                if not source_etl:
                    # USE ONE SOURCE ETL, OTHERWISE WE MAKE TOO MANY KEYS
                    source_etl = etl
                    if (
                        not source_etl.source.source
                    ):  # FIX ONCE TC LOGGER IS USING "tc" PREFIX FOR KEYS
                        source_etl.source.type = "join"
                        source_etl.source.source = {"id": "tc"}

                normalized = Data(
                    task={"id": task_id},
                    etl={
                        "id": line_number,
                        "source": source_etl,
                        "type": "join",
                        "timestamp": Date.now(),
                        "error": "not found",
                        "machine": machine_metadata,
                    },
                )

                output.append(normalized)

                continue

            # if not tc_message.status.runs.last().resolved:
            # UPDATE TASK STATUS (tc_message MAY BE OLD)
            status_url = strings.expand_template(TC_STATUS_URL, {"task_id": task_id})
            task_status = http.get_json(status_url, retry=TC_RETRY, session=session)
            consume(task_status, "status.taskId")
            temp_runs, task_status.status.runs = (
                task_status.status.runs,
                Null,
            )  # set_default() will screw `runs` up
            set_default(tc_message.status, task_status.status)
            tc_message.status.runs = [
                set_default(r, tc_message.status.runs[ii])
                for ii, r in enumerate(temp_runs)
            ]
            if not tc_message.status.runs.last().resolved:
                Log.error(
                    TRY_AGAIN_LATER, reason='task still runnning (not "resolved")'
                )

            normalized = _normalize(source_key, task_id, tc_message, task, resources)

            # get the artifact list for the taskId
            try:
                artifacts = normalized.task.artifacts = http.get_json(
                    strings.expand_template(TC_ARTIFACTS_URL, {"task_id": task_id}),
                    retry=TC_RETRY,
                ).artifacts
            except Exception as e:
                Log.error(
                    TRY_AGAIN_LATER,
                    reason="Can not get artifacts for task " + task_id,
                    cause=e,
                )

            for a in artifacts:
                a.url = strings.expand_template(
                    TC_ARTIFACT_URL, {"task_id": task_id, "path": a.name}
                )
                a.expires = Date(a.expires)
                if a.name.endswith("/live_backing.log"):
                    try:
                        read_actions(source_key, normalized, a.url)
                    except Exception as e:
                        if normalized.task.run.status != "completed":
                            # THIS IS EXPECTED WHEN THE TASK IS IN AN ERROR STATE, CHECK IT AND IGNORE
                            pass
                        elif "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in e:
                            # HAPPENS WHEN ETL RUNS BEFORE AWS HAS MACHINE FULLY SETUP
                            Log.error(TRY_AGAIN_LATER, reason="Unhappy network state", cause=e)
                        elif "Error -3 while decompressing data: incorrect data check" in e:
                            # HAPPENS WHEN ETL RUNS BEFORE AWS HAS MACHINE FULLY SETUP
                            Log.error(TRY_AGAIN_LATER, reason="Unhappy network state", cause=e)
                        elif TRY_AGAIN_LATER in e:
                            Log.error(
                                "Aborting processing of {{url}} for key={{key}}",
                                url=a.url,
                                key=source_key,
                                cause=e,
                            )
                        else:
                            # THIS IS EXPECTED WHEN THE TASK IS IN AN ERROR STATE, CHECK IT AND IGNORE
                            Log.error(
                                "Problem reading artifact {{url}} for key={{key}}",
                                url=a.url,
                                key=source_key,
                                cause=e,
                            )
                elif a.name.endswith("/resource-usage.json"):
                    with suppress_exception:
                        normalized.resource_usage = normalize_resource_usage(a.url)
            # FIX THE ETL
            if not source_etl:
                # USE ONE SOURCE ETL, OTHERWISE WE MAKE TOO MANY KEYS
                source_etl = etl
                if (
                    not source_etl.source.source
                ):  # FIX ONCE TC LOGGER IS USING "tc" PREFIX FOR KEYS
                    source_etl.source.type = "join"
                    source_etl.source.source = {"id": "tc"}
            normalized.etl = {
                "id": line_number,
                "source": source_etl,
                "type": "join",
                "timestamp": Date.now(),
                "machine": machine_metadata,
            }

            tc_message.artifact = "." if tc_message.artifact else Null
            if normalized.task.id in seen_tasks:
                try:
                    assertAlmostEqual(
                        [tc_message, task, artifacts],
                        seen_tasks[normalized.task.id],
                        places=11,
                    )
                except Exception as e:
                    Log.error("Not expected", cause=e)
            else:
                tc_message._meta = Null
                tc_message.runs = Null
                tc_message.runId = Null
                tc_message.artifact = Null
                seen_tasks[normalized.task.id] = [tc_message, task, artifacts]

            output.append(normalized)
        except Exception as e:
            e = Except.wrap(e)
            if TRY_AGAIN_LATER in e:
                raise e
            elif mo_math.round(e.params.code, decimal=-2) == 500:
                Log.error(
                    TRY_AGAIN_LATER, reason="error code " + text(e.params.code)
                )
            else:
                Log.warning(
                    "TaskCluster line not processed for key {{key}}: {{line|quote}}",
                    key=source_key,
                    line=line,
                    cause=e,
                )

    keys = destination.extend({"id": etl2key(t.etl), "value": t} for t in output)
    return keys


def read_actions(source_key, normalized, url):
    if DISABLE_LOG_PARSING:
        return
    try:
        all_log_lines = http.get(url).get_all_lines(encoding=Null)
        normalized.action = process_tc_live_backing_log(
            source_key, all_log_lines, url, normalized
        )
    except Exception as e:
        e = Except.wrap(e)
        if "Connection broken: error(104," in e:
            Log.error(TRY_AGAIN_LATER, reason="broken connection")
        elif "'Connection aborted.', BadStatusLine" in e:
            Log.error(TRY_AGAIN_LATER, reason="broken connection")
        elif "Read timed out" in e:
            Log.error(TRY_AGAIN_LATER, reason="read timeout")
        elif "Failed to establish a new connection" in e:
            Log.error(TRY_AGAIN_LATER, reason="could not connect")
        elif "An existing connection was forcibly closed by the remote host" in e:
            Log.error(TRY_AGAIN_LATER, reason="text log was forcibly closed")
        else:
            Log.error("problem processing {{key}}", key=source_key, cause=e)


def _normalize(source_key, task_id, tc_message, task, resources):
    output = Data()
    set_default(task, consume(tc_message, "status"))

    if isinstance(task.extra.partials, list):
        if len(task.extra.partials) > 1 and task.extra.partials[0].locale == None:
            Log.warning(
                "task.extra.partials has {{num}} instances! key={{key}}",
                num=len(task.extra.partials),
                key=source_key,
            )
        task.extra.partials = set_default({}, *task.extra.partials)

    output.task.id = task_id
    output.task.label = task.tags.label  # CONSUMED AGAIN, AS run.key
    output.task.kind = consume(task, "tags.kind")
    output.task.test_type = consume(task, "tags.test-type")
    output.task.created = Date(consume(task, "created"))
    output.task.deadline = Date(consume(task, "deadline"))
    output.task.dependencies = unwraplist(consume(task, "dependencies"))
    output.task.expires = Date(consume(task, "expires"))
    output.task.maxRunTime = consume(task, "payload.maxRunTime")
    if output.task.maxRunTime == "hello":
        output.task.maxRunTime = Null

    env = consume(task, "payload.env")
    output.task.env = _object_to_array(env, "name", "value")

    features = consume(task, "payload.features")
    try:
        if isinstance(features, text):
            output.task.features = [features]
        elif all(isinstance(v, bool) for v in features.values()):
            output.task.features = [k if v else "!" + k for k, v in features.items()]
        else:
            Log.error("Unexpected features: {{features|json}}", features=features)
    except Exception:
        Log.warning("Unexpected features: {{features|json}}", features=features)

    output.task.cache = _object_to_array(
        consume(task, "payload.cache"), "name", "value"
    )
    output.task.requires = consume(task, "requires")
    output.task.capabilities = consume(task, "payload.capabilities")

    image = consume(task, "payload.image")
    if isinstance(image, text):
        output.task.image = {"path": image}
    else:
        output.task.image = image

    output.task.priority = consume(task, "priority")
    output.task.provisioner.id = consume(task, "provisionerId")
    output.task.retries.remaining = consume(task, "retriesLeft")
    output.task.retries.total = consume(task, "retries")
    output.task.routes = consume(task, "routes")

    run_id = coalesce(consume(tc_message, "runId"), len(task.runs) - 1)
    output.task.run = _normalize_task_run(task.runs[run_id])
    output.task.runs = list(map(_normalize_task_run, consume(task, "runs")))
    output.task.reboot = consume(task, "payload.reboot")

    output.task.scheduler.id = consume(task, "schedulerId")
    output.task.scopes = consume(task, "scopes")
    output.task.state = consume(task, "state")
    output.task.group.id = consume(task, "taskGroupId")
    output.task.version = consume(tc_message, "version")
    output.task.worker.group = consume(tc_message, "workerGroup")
    output.task.worker.id = consume(tc_message, "workerId")
    output.task.worker.type = consume(task, "workerType")

    output.task.manifest.task_id = consume(task, "payload.taskid_of_manifest")
    output.task.manifest.update = consume(task, "payload.update_manifest")
    output.task.beetmove.task_id = coalesce_w_conflict_detection(
        source_key,
        consume(task, "payload.taskid_to_beetmove"),
        consume(task, "payload.properties.taskid_to_beetmove"),
    )

    # DELETE JUNK
    consume(task, "payload.routes")
    consume(task, "payload.log")
    consume(task, "payload.suffixes")
    consume(task, "payload.upstreamArtifacts")
    consume(task, "extra.env")
    output.task.signing.cert = (
        coalesce(*listwrap(consume(task, "payload.signing_cert"))),
    )  # OFTEN HAS NULLS
    output.task.parent.id = coalesce_w_conflict_detection(
        source_key,
        consume(task, "parent_task_id"),
        consume(task, "payload.properties.parent_task_id"),
        consume(task, "extra.parent"),
    )
    output.task.parent.artifacts_url = consume(
        task, "payload.parent_task_artifacts_url"
    )

    # MOUNTS
    output.task.mounts = consume(task, "payload.mounts")

    try:
        artifacts = consume(task, "payload.artifacts")
        if isinstance(artifacts, list):
            for a in artifacts:
                if not a.name:
                    if not a.path:
                        Log.error("expecting name, or path of artifact")
                    else:
                        a.name = a.path
            output.task.artifacts = artifacts
        else:
            output.task.artifacts = _object_to_array(artifacts, "name")

        artifact_id = consume(task, "payload.artifact_id")
        if artifact_id:
            output.task.artifacts += [{"id": artifact_id}]

        consume(task, "payload.artifactMap")

    except Exception as e:
        Log.warning(
            "artifact format problem in {{key}}:\n{{artifact|json|indent}}",
            key=source_key,
            artifact=task.payload.artifacts,
            cause=e,
        )
    output.task.cache = _object_to_array(task.payload.cache, "name", "path")
    try:
        command = consume(task, "payload.command")
        cmd = consume(task, "payload.cmd")
        command = [
            cc for c in (command if command else cmd) for cc in listwrap(c)
        ]  # SOMETIMES A LIST OF LISTS
        output.task.command = " ".join(
            map(convert.string2quote, map(text.strip, command))
        )
    except Exception as e:
        Log.error("problem", cause=e)

    set_build_info(source_key, output, task, env, resources)
    _normalize_run(source_key, output, task, env)

    # VERIFY DUPLICATE
    treeherder_platform = consume(task, "extra.treeherder-platform")
    if treeherder_platform != None:
        pair = treeherder_platform.split("/")
        try:
            if (
                pair[0] == output.treeherder.machine.platform
                and output.treeherder.collection[pair[1]]
            ):
                pass
            else:
                Log.warning(
                    "extra.treeherder platform does not match treeherder for key {{key}}",
                    key=source_key,
                )
        except Exception:
            Log.warning(
                "extra.treeherder platform does not match treeherder for key {{key}}",
                key=source_key,
            )

    output.task.tags = get_tags(source_key, output.task.id, task)

    output.build.type = unwraplist(list(set(listwrap(output.build.type))))
    output.run.type = unwraplist(list(set(listwrap(output.run.type))))

    # PROPERTIES THAT HAVE NOT BEEN HANDLED
    remaining_keys = (
        set(k for k, v in task.leaves())
        - new_seen_tc_properties
    )
    if remaining_keys:
        list(map(new_seen_tc_properties.add, remaining_keys))
        Log.warning(
            "Some properties ({{props|json}}) are not consumed while processing key {{key}}",
            key=source_key,
            props=remaining_keys,
        )

    # TODO: make a list of required properties for all tests and builds
    if not output.build.platform and output.run.name.startswith("test-"):
        Log.warning(
            "Task is missing build.platform while processing key {{key}}",
            key=source_key,
        )

    return output


def _normalize_task_run(run):
    output = Data()
    output.reason_created = run.reasonCreated
    output.id = run.id
    output.scheduled = Date(run.scheduled)
    output.start_time = Date(run.started)
    output.status = run.reasonResolved
    output.end_time = Date(run.resolved)
    output.duration = Date(run.resolved) - Date(run.started)
    output.state = run.state
    output.worker.group = run.workerGroup
    output.worker.id = run.workerId
    return output


def _normalize_run(source_key, normalized, task, env):
    """
    Get the run object that contains properties that describe the run of this job
    :param task: The task definition
    :return: The run object
    """

    run_type = []

    # PARSE TEST SUITE NAME
    suite = consume(task, "extra.suite")
    if isinstance(suite, text):
        suite = wrap({"name": suite})
    test = suite.name.lower()

    # FLAVOR
    flavor = suite.flavor.lower()
    if test == flavor:
        flavor = Null
    elif flavor.startswith(test + "-"):
        flavor = flavor[len(test) + 1 : :]

    # FLAVOURS
    if flavor and "-e10s" in flavor:
        flavor = flavor.replace("-e10s", "").strip()
        if not flavor:
            flavor = Null
        run_type += ["e10s"]

    if flavor == "chunked":
        flavor = Null
    elif flavor and "-chunked" in flavor:
        flavor = flavor.replace("-chunked", "").strip()
        if not flavor:
            flavor = Null

    # CHUNK NUMBER
    chunk = Null
    path = test.split("-")
    if mo_math.is_integer(path[-1]):
        chunk = int(path[-1])
        test = "-".join(path[:-1])
    chunk = coalesce_w_conflict_detection(
        source_key,
        consume(task, "extra.chunks.current"),
        consume(task, "payload.properties.THIS_CHUNK"),
        chunk,
    )
    test = coalesce_w_conflict_detection(
        source_key, test, consume(task, "tags.test-type")
    )

    if test == None:
        fullname = Null
    elif flavor == None:
        fullname = test
    else:
        fullname = test + "-" + flavor

    # TRIGGER
    # trigger = coalesce_w_conflict_detection(
    #     source_key,
    #     consume(task, "context.triggeredBy"),
    #     consume(task, "context.firedBy"),
    #     consume(task, "triggeredBy"),
    #     consume(task, "firedBy")
    # )
    consume(task, "tags.retrigger")

    metadata_name = consume(task, "metadata.name")
    set_default(
        normalized,
        {
            "run": {
                "key": coalesce(
                    consume(task, "payload.buildername"), consume(task, "tags.label")
                ),
                "name": metadata_name,
                "machine": normalized.treeherder.machine,
                "suite": {"name": test, "flavor": flavor, "fullname": fullname},
                "chunk": chunk,
                "type": unwraplist(list(set(run_type))),
                # "trigger": trigger,
                "timestamp": normalized.task.run.start_time,
            }
        },
        decode_metatdata_name(source_key, metadata_name),
    )


def set_build_info(source_key, normalized, task, env, resources):
    """
    Get a build object that describes the build
    :type normalized: Data
    :param task: The task definition
    :return: The build object
    """

    if task.workerType.startswith("dummy-type"):
        task.workerType = "dummy-type"

    build_type = consume(task, "extra.build_type")

    set_default(
        normalized,
        {
            "build": {
                "id": coalesce_w_conflict_detection(
                    source_key,
                    consume(task, "extra.buildid"),
                    consume(task, "payload.releaseProperties.buildid"),
                ),
                "name": consume(task, "extra.build_name"),
                "product": coalesce_w_conflict_detection(
                    source_key,
                    consume(task, "payload.properties.product").lower(),
                    consume(task, "payload.releaseProperties.appName").lower(),
                    consume(task, "tags.build_props.product").lower(),
                    consume(task, "extra.build_props.product").lower(),
                    task.extra.treeherder.productName.lower(),
                    consume(task, "extra.build_product").lower(),
                    consume(task, "extra.product")
                    .lower()
                    .replace("devedition", "firefox"),
                    consume(task, "payload.product").lower(),
                    "firefox"
                    if is_data(task.extra.suite)
                    and task.extra.suite.name.startswith("firefox")
                    else Null,
                    "firefox"
                    if any(
                        r.startswith("index.gecko.v2.try.latest.firefox.")
                        for r in normalized.task.routes
                    )
                    else Null,
                    consume(task, "extra.app-name"),
                ),
                "platform": coalesce_w_conflict_detection(
                    source_key,
                    _simplify_platform(
                        consume(task, "payload.releaseProperties.platform")
                    ),
                    _simplify_platform(task.extra.treeherder.build.platform),
                    _simplify_platform(task.extra.treeherder.machine.platform),
                    consume(task, "extra.build_props.platform"),
                    consume(task, "extra.platform"),
                ),
                # MOZILLA_BUILD_URL looks like this:
                # https://queue.taskcluster.net/v1/task/e6TfNRfiR3W7ZbGS6SRGWg/artifacts/public/build/target.tar.bz2
                "url": env.MOZILLA_BUILD_URL,
                "revision": coalesce_w_conflict_detection(
                    source_key,
                    consume(task, "tags.build_props.revision"),
                    consume(task, "extra.build_props.revision"),
                    consume(task, "payload.sourcestamp.revision"),
                    consume(task, "payload.properties.revision"),
                    env.GECKO_HEAD_REV,
                ),
                "type": listwrap({"dbg": "debug"}.get(build_type, build_type)),
                "version": coalesce_w_conflict_detection(
                    source_key,
                    consume(task, "extra.build_props.version"),
                    consume(task, "tags.build_props.version"),
                    consume(task, "payload.releaseProperties.appVersion"),
                    consume(task, "payload.app_version"),
                ),
                "channel": coalesce_w_conflict_detection(
                    source_key,
                    consume(task, "payload.properties.channels"),
                    consume(task, "extra.channel"),
                    consume(task, "payload.channel"),
                ),
            }
        },
    )

    if "-ccov" in normalized.build.platform:
        normalized.build.platform = normalized.build.platform.replace("-ccov", "")
        normalized.build.type += ["ccov"]
    if "-jsdcov" in normalized.build.platform:
        normalized.build.platform = normalized.build.platform.replace("-jsdcov", "")
        normalized.build.type += ["jsdcov"]

    normalized.build.branch = coalesce_w_conflict_detection(
        source_key,
        consume(task, "tags.build_props.branch"),
        consume(task, "extra.build_props.branch"),
        consume(task, "payload.releaseProperties.branch"),
        consume(task, "payload.branch"),
        consume(task, "payload.sourcestamp.branch").split("/")[-1],
        env.GECKO_HEAD_REPOSITORY.strip("/").split("/")[
            -1
        ],  # will look like "https://hg.mozilla.org/try/"
        consume(task, "payload.properties.repo_path").split("/")[-1],
        env.MH_BRANCH,
    )
    normalized.build.revision12 = normalized.build.revision[0:12]

    if normalized.build.revision:
        candidate = {
            "branch": {"name": normalized.build.branch},
            "changeset": {"id": normalized.build.revision},
        }
        normalized.repo = minimize_repo(resources.hg.get_revision(wrap(candidate)))
        if not normalized.repo:
            if normalized.build.branch not in UNKNOWN_BRANCHES:
                Log.warning(
                    "No repo found for {{rev}} while processing key={{key}}",
                    key=source_key,
                    rev=candidate,
                )
            normalized.repo = candidate
            normalized.repo.changeset.id12 = normalized.build.revision[:12]
        elif not normalized.repo.push.date:
            Log.warning(
                "did not assign a repo.push.date for source_key={{key}}", key=source_key
            )
        normalized.build.date = normalized.repo.push.date

    normalized.run.phabricator_diff = consume(
        task, "extra.code-review.phabricator-diff"
    )

    treeherder = consume(task, "extra.treeherder")
    if treeherder:
        for l, v in treeherder.leaves():
            normalized.treeherder[l] = v

    # BUILD TYPES ARE SEPARATED BY DASH (-) AND SLASH (/)
    collection = normalized.treeherder.collection = wrap(
        {
            kkk: v
            for k, v in treeherder.collection.items()
            for kk in k.split("/")
            for kkk in kk.split("-")
        }
    )
    for k, v in BUILD_TYPES.items():
        if collection[k]:
            normalized.build.type += v

    diff = collection.keys() - BUILD_TYPE_KEYS
    if diff:
        Log.warning(
            "new collection type of {{type}} while processing key {{key}}",
            type=diff,
            key=source_key,
        )

    # FIND BUILD TASK
    # if treeherder.jobKind == "test":
    #     build_task = get_build_task(source_key, resources, normalized)
    #     if build_task:
    #         if DEBUG:
    #             Log.note(
    #                 "Got build {{build}} for test {{test}}",
    #                 build=build_task.task.id,
    #                 test=normalized.task.id,
    #             )
    #         minimize_task(build_task)
    #         set_default(normalized.build, build_task)


MISSING_BUILDS = set()


def get_build_task(source_key, resources, normalized_task):
    # "revision12":"571286200177",
    # "url":"https://queue.taskcluster.net/v1/task/J4jnKgKAQieAhwvSQBKa3Q/artifacts/public/build/target.tar.bz2",
    # "platform":"linux64",
    # "branch":"graphics",
    # "date":1484242475,
    # "type":"opt",
    # "revision":"571286200177ae7ddfa1893c6b42853b60f2e81e"

    build_task_id = listwrap(
        coalesce(
            strings.between(normalized_task.build.url, "task/", "/"),
            normalized_task.task.dependencies,
        )
    )
    if not build_task_id:
        Log.warning(
            "Could not find build.url {{task}} in {{key}}",
            task=normalized_task.task.id,
            key=source_key,
        )
        return Null
    try:
        response = http.post_json(
            URL(
                value=resources.local_es_node.host,
                port=coalesce(resources.local_es_node.port, 9200),
                path="task/task/_search",
            ),
            headers={"Content-Type": mimetype.JSON},
            data={
                "query": {"terms": {"task.id": build_task_id}},
                "from": 0,
                "size": 10,
            },
            retry={"times": 3, "sleep": 15},
        )
    except Exception as e:
        Log.warning(
            "Failure to get build task while processing {{key}}",
            key=source_key,
            cause=e,
        )
        return Null

    candidates = jx.sort(
        [
            h._source
            for h in response.hits.hits
            if h._source.treeherder.jobKind == "build"
        ],
        "run.start_time",
    )
    if not candidates:
        if not any(b in MISSING_BUILDS for b in build_task_id):
            Log.alert(
                "Could not find any build task {{build}} for test {{task}} in {{key}}",
                task=normalized_task.task.id,
                build=build_task_id,
                key=source_key,
            )
            MISSING_BUILDS.update(build_task_id)
        return Null

    if normalized_task.build.revision12 != None:
        candidate = candidates.filter(
            lambda c: c.build.revision12 == normalized_task.build.revision12
        ).last()

        if not candidate:
            # if normalized_task.repo.branch.name in ["mozilla-central"]:
            #     # THE TASK GROUP IS VERY COMPLICATED, DO NOT BOTHER COMPLAINING
            #     # TODO: REMOVE AFTER 2018, THEN FIGURE OUT IF THE TEST CAN RESOLVE TO THE CORRECT BUILD
            #     return None
            Log.warning(
                "Could not find matching build task {{build}} for test {{task}} in {{key}}",
                task=normalized_task.task.id,
                build=build_task_id,
                key=source_key,
            )
            return Null
    else:
        candidate = candidates.last()
    return candidate


def get_tags(source_key, task_id, task, parent=None):
    tags = []
    # SPECIAL CASES
    platforms = consume(task, "payload.properties.platforms")
    if isinstance(platforms, text):
        platforms = list(map(text.strip, platforms.split(",")))
        tags.append({"name": "platforms", "value": platforms})
    link = consume(task, "payload.link")
    if link:
        tags.append({"name": "link", "value": link})

    consume(task, "extra.action.context.parameters")  # TOO MANY COMBINATIONS
    consume(task, "extra.action.context.input")
    consume(task, "payload.l10n_bump_info")

    # VARIOUS LOCATIONS TO FIND TAGS
    t = consume(task, "tags").leaves()
    m = consume(task, "metadata").leaves()
    e = consume(task, "extra").leaves()
    p = consume(task, "payload.properties").leaves()
    g = [(k, consume(task.payload, k)) for k in PAYLOAD_PROPERTIES]

    tags.extend({"name": k, "value": v} for k, v in t)
    tags.extend({"name": k, "value": v} for k, v in m)
    tags.extend({"name": k, "value": v} for k, v in e)
    tags.extend({"name": k, "value": v} for k, v in p)
    tags.extend({"name": k, "value": v} for k, v in g)

    clean_tags = []
    for t in tags:
        # ENSURE THE VALUES ARE UNICODE
        if parent:
            t["name"] = parent + "." + t["name"]
        v = t["value"]
        if v == None:
            continue
        elif isinstance(v, list):
            if len(v) == 1:
                v = v[0]
                if is_data(v):
                    for tt in get_tags(
                        source_key, task_id, Data(tags=v), parent=t["name"]
                    ):
                        clean_tags.append(tt)
                    continue
                elif not isinstance(v, text):
                    v = value2json(v)
            # elif all(isinstance(vv, (text, float, int)) for vv in v):
            #     pass  # LIST OF PRIMITIVES IS OK
            else:
                v = value2json(v)
        elif not isinstance(v, text):
            v = value2json(v)
        t["value"] = v
        verify_tag(source_key, task_id, t)
        clean_tags.append(t)

    return clean_tags


def verify_tag(source_key, task_id, t):
    if not isinstance(t["value"], text):
        Log.error("Expecting unicode")
    if t["name"] not in KNOWN_TAGS:
        Log.warning(
            "unknown task tag {{tag|json}} while processing {{task_id}} in {{key}}",
            key=source_key,
            id=task_id,
            tag=t,
        )
        KNOWN_TAGS.add(t["name"])


null = Null
KNOWN_COALESCE_CONFLICTS = {
    (
        null,
        null,
        null,
        null,
        null,
        null,
        "firefox",
        null,
        null,
        null,
        "browser",
    ): "firefox",
    (
        null,
        null,
        null,
        null,
        "mozilla-central",
        null,
        "comm-central",
    ): "mozilla-central",
    (
        null,
        "thunderbird",
        null,
        null,
        null,
        null,
        "firefox",
        null,
        null,
        null,
        null,
    ): "thunderbird",
    (null, null, null, null, "mozilla-beta", null, "comm-beta"): "mozilla-beta",
    (null, null, null, null, null, "mozilla-beta", null, "comm-beta"): "mozilla-beta",
    (
        null,
        null,
        null,
        null,
        null,
        "mozilla-central",
        null,
        "try-comm-central",
    ): "mozilla-central",
    (
        null,
        null,
        null,
        null,
        null,
        "mozilla-central",
        null,
        "comm-central",
    ): "mozilla-central",
    (null, null, null, null, null, "mozilla-beta", null, "comm-beta"): "mozilla-beta",
    (
        null,
        null,
        null,
        null,
        null,
        "mozilla-esr60",
        null,
        "comm-esr60",
    ): "mozilla-esr60",
    (
        null,
        null,
        null,
        null,
        null,
        "gecko-dev.git",
        null,
        "mozilla-beta",
    ): "gecko-dev.git",
    (
        null,
        null,
        null,
        null,
        null,
        "gecko-dev.git",
        null,
        "mozilla-release",
    ): "gecko-dev.git",
    (null, null, null, null, null, "try", null, "try-comm-central"): "try",
    ("jsreftest", "reftest"): "jsreftest",
    (
        "win64-aarch64-devedition",
        null,
        "windows2012-aarch64-devedition",
        null,
        null,
    ): "win64-aarch64-devedition",
    ("android-x86_64", null, "android", null, null): "android-x86_64",
    (
        null,
        null,
        null,
        null,
        null,
        null,
        "thunderbird",
        null,
        null,
        null,
        "mail",
    ): "thunderbird",
    (
        null, null, null, null,
        null, "mozilla-esr68", null, "comm-esr68"
    ): "mozilla-esr68",
}


def coalesce_w_conflict_detection(source_key, *args):
    if len(args) < 2:
        Log.error("bad call to coalesce, expecting source_key as first parameter")

    output = KNOWN_COALESCE_CONFLICTS.get(args, Null)
    if output is not Null:
        return output

    output = Null
    for a in args:
        if a == None:
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


def _scrub(record, name):
    value = record[name]
    record[name] = None
    if value == "-" or value == "":
        return None
    else:
        return unwraplist(value)


def _object_to_array(value, key_name, value_name=None):
    try:
        if value_name == None:
            return unwraplist([set_default(v, {key_name: k}) for k, v in value.items()])
        else:
            return unwraplist(
                [
                    {
                        key_name: k,
                        value_name: strings.limit(v, 1000)
                        if isinstance(v, text)
                        else v,
                    }
                    for k, v in value.items()
                ]
            )
    except Exception as e:
        Log.error("unexpected", cause=e)


def _simplify_platform(platform):
    """
    Used to simplify the number of distracting warnings
    :param platform: a string
    :return: A simpler version of platform, or itself
    """
    return SIMPLER_PLATFORMS.get(platform, platform)


SIMPLER_PLATFORMS = {
    "android-api-16": "android",
    "android-aarch64": "android",
    "android-4-2-x86": "android",
    "android-5-0-aarch64": "android",
    "android-5-0-x86_64": "android",
    "android-4-0-armv7-api16-old-id": "android",
    "android-4-0-armv7-api16": "android",
    "android-x86": "android",
    "osx-cross": "macosx64",
    "osx-shippable": "macosx64",
    "osx-cross-devedition": "macosx64",
    "macosx64-devedition": "macosx64",
    "macosx64-shippable": "macosx64",
    "win32-devedition": "win32",
    "win32-shippable": "win32",
    "win64-aarch64-devedition": "win64",
    "win64-aarch64": "win64",
    "win64-devedition": "win64",
    "win64-shippable": "win64",
    "windows2012-32-shippable": "win32",
    "windows2012-32-devedition": "win32",
    "windows2012-32": "win32",
    "windows2012-64-devedition": "win64",
    "windows2012-64-shippable": "win64",
    "windows2012-64": "win64",
    "windows2012-aarch64-devedition": "win64",
    "windows2012-aarch64-shippable": "win64",
    "windows2012-aarch64": "win64",
    "linux32-devedition": "linux32",
    "linux32-shippable": "linux32",
    "linux64-shippable": "linux64",
    "linux64-snap": "linux64",
    "linux-devedition": "linux32",
    "linux-shippable": "linux32",
    "linux": "linux32",
}

BUILD_TYPES = {
    "all": ["all"],
    "asan": ["asan"],
    "ccov": ["ccov"],
    "debug": ["debug"],
    "fips": ["fips"],
    "fuzz": ["fuzz"],
    "gyp": ["gyp"],
    "jsdcov": ["jsdcov"],
    "lsan": ["lsan"],
    "lto": ["lto"],  # LINK TIME OPTIMIZATION
    "nightly": [],
    "make": ["make"],
    "memleak": ["memleak"],
    "nostylo": ["stylo-disabled"],
    "opt": ["opt"],
    "pgo": ["pgo"],
    "raptor": ["raptor"],
    "release": [],
    "tsan": ["tsan"],
    "ubsan": ["ubsan"],
}

BUILD_TYPE_KEYS = set(BUILD_TYPES.keys())
PAYLOAD_PROPERTIES = {
    "actions",
    "aliases_entries",
    "apks.armv7_v15",
    "apks.x86",
    "appVersion",
    "archive_domain",
    "artifactsTaskId",
    "artifactMap",
    "balrog_api_root",
    "behavior",
    "bouncer_products",
    "build_number",
    "certificate_alias",
    "chain",
    "CHANNEL",
    "channel_names",
    "commit",
    "contact",
    "context",
    "created",
    "deadline",
    "description",
    "desiredResolution",
    "dont_build",
    "dontbuild",
    "download_domain",
    "dry_run",
    "encryptedEnv",
    "entitlements-url",
    "en_us_binary_url",
    "expires",
    "fake",
    "google_play_track",
    "graphs",  # POINTER TO graph.json ARTIFACT
    "ignore_closed_tree",
    "is_partner_repack_public",
    "l10n_changesets",
    "locales",
    "locale",
    "merge_info.base_tag",
    "merge_info.copy_files",
    "merge_info.end_tag",
    "merge_info.from_branch"
    "merge_info.from_repo",
    "merge_info.merge_old_head",
    "merge_info.replacements",
    "merge_info.to_branch",
    "merge_info.to_repo",
    "merge_info.version_files",
    "merge_info.version_files_suffix",
    "merge_info.version_suffix",
    "mar_tools_url",
    "next_version",
    "NO_BBCONFIG",
    "onExitStatus",
    "osGroups",
    "partials",
    "partial_versions",

    "platforms",
    "publish_rules",
    "purge-caches-exit-status",
    "purpose",
    "release_eta",
    "release_name",
    "release_promotion",
    "releaseProperties.hashType",
    "repack_manifests_url",
    "require_mirrors",
    "retry-exit-status",
    "revision",
    "rollout_percentage",
    "rules_to_update",
    "target_store",
    "timeout",
    "see",
    "script_repo_revision",
    "signingManifest",
    "source_repo",
    "sourcestamp.repository",
    "ssh_user",
    "stage-product",
    "submission_entries",
    "summary",
    "supersederUrl",
    "tag_info",
    "template_key",
    "THIS_CHUNK",
    "TOTAL_CHUNKS",
    "tuxedo_server_url",
    "unsignedArtifacts",
    "update_line",
    "upload_date",
    "uuid_manifest",
    "VERIFY_CONFIG",
    "version_bump_info",
    "version",
}

KNOWN_TAGS = {
    "action.name",
    "action.context",
    "action.context.clientId",
    "action.context.input.tasks",
    "action.context.taskGroupId",
    "action.context.taskId",
    "android-stuff",
    "aus-server",
    "archive-prefix",
    "branch-prefix",
    "build_props.build_number",
    "build_props.release_eta",
    "build_props.locales",
    "build_props.mozharness_changeset",
    "build_props.partials",
    "chainOfTrust.inputs.docker-image",
    "chunks.current",
    "chunks.total",
    "chunks",
    "CI",
    "code-review.phabricator-build-target",
    "context.firedBy",
    "context.flattenedDeep",
    "context.flettenedDeep",
    "context.taskId",
    "context.valueFromContext",
    "crater.crateName",
    "crater.toolchain.customSha",
    "crater.crateVers",
    "crater.taskType",
    "crater.toolchain.archiveDate",
    "crater.toolchain.channel",
    "crater.toolchainGitRepo",
    "crater.toolchainGitSha",
    "createdForUser",
    "cron",
    "data.base.sha",
    "data.base.user.login",
    "data.head.sha",
    "data.head.user.email",
    "description",
    "notify.email.link.href",
    "notify.email.link.text",
    "notify.ircChannelMessage",
    "en_us_installer_binary_url",
    "firedBy",
    "funsize.partials",
    "funsize.partials.branch",
    "funsize.partials.dest_mar",
    "funsize.partials.from_mar",
    "funsize.partials.locale",
    "funsize.partials.platform",
    "funsize.partials.previousBuildNumber",
    "funsize.partials.previousVersion",
    "funsize.partials.product",
    "funsize.partials.to_mar",
    "funsize.partials.toBuildNumber",
    "funsize.partials.toVersion",
    "funsize.partials.update_number",
    "github.branches",
    "github.events",
    "github_event",
    "github.env",
    "github.excludeBranches",
    "github.headBranch",
    "github.headRepo",
    "github.headRevision",
    "github.headUser",
    "github.baseBranch",
    "github.baseRepo",
    "github.baseRevision",
    "github.baseUser",
    "githubPullRequest",
    "generate_bz2_blob",
    "imageMeta.contextHash",
    "imageMeta.imageName",
    "imageMeta.level",
    "include-version",
    "index.data.hello",
    "index.data.nix_hash",
    "index.data.revision",
    "index.expires",
    "index.rank",
    "installer_path",
    "l10n_changesets",
    "last-watershed",
    "limit-locales",
    "link",
    "locations.mozharness",
    "locations.test_packages",
    "locations.build",
    "locations.img",
    "locations.mar",
    "locations.sources",
    "locations.symbols",
    "locations.tests",
    "mar-channel-id-override",
    "name",
    "nc_asset_name",
    "notification.task-defined.irc.notify_nicks",
    "notification.task-defined.irc.message",
    "notification.task-defined.log_collect",
    "notification.task-defined.ses.body",
    "notification.task-defined.ses.recipients",
    "notification.task-defined.ses.subject",
    "notification.task-defined.smtp.body",
    "notification.task-defined.smtp.recipients",
    "notification.task-defined.smtp.subject",
    "notification.task-defined.sns.message",
    "notification.task-defined.sns.arn",
    "notifications.task-completed.emails",
    "notifications.task-completed.message",
    "notifications.task-completed.ids",
    "notifications.task-completed.plugins",
    "notifications.task-completed.subject",
    "notifications.task-failed.emails",
    "notifications.task-failed.ids",
    "notifications.task-failed.message",
    "notifications.task-failed.plugins",
    "notifications.task-failed.subject",
    "notifications.task-exception.emails",
    "notifications.task-exception.message",
    "notifications.task-exception.ids",
    "notifications.task-exception.plugins",
    "notifications.task-exception.subject",
    "notify.email.subject",
    "notify.email.content",
    "npmCache.url",
    "npmCache.expires",
    "objective",
    "os",
    "override-certs",
    "owner",
    "partial_versions",
    "partials",
    "partials.artifact_name",
    "partials.buildid",
    "partials.locale",
    "partials.platform",
    "partials.previousBuildNumber",
    "partials.previousVersion",
    "partner_path",
    "payload.dry_run",
    "payload.commit",
    "payload.release_name",
    "platforms",
    "previous-archive-prefix",
    "repack_id",
    "repack_ids",
    "repack_suffix",
    "retrigger",
    "schedule_at",
    "signed_installer_url",
    "signing.signature",
    "source",
    "suite.flavor",
    "suite.name",
    "tasks_for",
    "test-type",
    "treeherderEnv",
    "updater-platform",
    "upload_to_task_id",
    "url.busybox",
    "useCloudMirror",
    "who",
    "worker-implementation",
} | PAYLOAD_PROPERTIES


def consume(props, key):
    output, props[key] = props[key], Null
    return output


UNKNOWN_BRANCHES = [
    "android-components",
    "ci-taskgraph",
    "servo-main",
    "servo-try",
    "servo-prs",
    "fxapom",
    "reference-browser",
    "fenix",
]
