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

from mo_future import text
import ast
import re

from mo_dots import wrap, Data, coalesce, set_default, unwraplist, listwrap
from mo_json import json2value, value2json
from mo_logs.exceptions import suppress_exception
from mo_logs import Log, strings
from mo_math import is_integer, is_number
from pyLibrary import convert
from jx_elasticsearch import elasticsearch
from mo_times.dates import Date, unicode2Date

BUILDBOT_LOGS = "http://builddata.pub.build.mozilla.org/builddata/buildjson/"


class BuildbotTranslator(object):

    def __init__(self):
        self.unknown_platforms=[]

    def parse(self, data):
        data = wrap(data)
        output = Data()

        if data.build.times:
            if len(data.build.times) != 2:
                Log.error("not expected")
            data.build.result = data.results
            data = data.build

        if isinstance(data.properties, list):
            props = data.properties = Data(**{a: b for a, b, c in data.properties})
        else:
            props = data.properties.copy()

        output.action.reason = data.reason
        output.action.request_time = Date(data.requesttime)
        output.action.start_time = coalesce(Date(data.starttime), Date(data.times[0]), Date(consume(data, "insertion_time")))
        output.action.end_time = coalesce(Date(data.endtime), Date(data.times[1]))
        output.action.buildbot_status = STATUS_CODES[coalesce(data.result, consume(data, "status"))]

        if data.actions:
            Log.error("See actions")

        if len({"buildername", "platform", "product"} - data.keys()) == 0:
            props = data
        if not props or not props.buildername:
            output.properties, output.other = normalize_other(props)
            return output

        if consume(props, "talos"):
            output.action.type = "talos"

        # TASK CLUSTER ID
        bbb_task_id = data.reason[24:] if data.reason.startswith("Created by BBB for task ") else None
        output.task.id = coalesce(consume(props, "taskId"), bbb_task_id)

        output.action.job_number = coalesce(consume(props, "buildnumber"), consume(props, "job_number"), consume(props, "build_number"))
        for k, v in consume(props, "request_times").items():
            output.action.requests += [{"request_id": int(k), "timestamp": v}]
        consume(props, "request_ids")

        output.run.timestamp = Date(consume(data, "timestamp"))
        output.run.key = buildername = consume(props, "buildername")
        consume(props, "key")  # THE PULSE KEY

        if buildername.startswith("TB "):
            buildername = buildername[3:]

        try:
            ratio = RATIO_PATTERN.match(buildername.split("_")[-1])
            if ratio:
                output.action.step = ratio.groups()[0]
        except Exception as e:
            Log.error("problem in\n{{data|json}}", data=data, cause=e)

        # SCRIPT
        output.run.script.url = consume(props, "script_repo_url")
        output.run.script.revision = consume(props, "script_repo_revision")

        # REPO AND REVISIONS
        raw_release = consume(props, "release")
        output.build.gaia_revision = consume(props, "gaia_revision")
        output.build.gaia_revision12 = output.build.gaia_revision[0:12]
        if props.gecko_revision:
            if props.revision and props.gecko_revision[0:12] != props.revision[0:12]:
                Log.error("expecting revision to be the gecko revision\n{{data}}", data=data)
            output.build.revision = coalesce(consume(props, "revision"), consume(props, "gecko_revision"), raw_release)
            output.build.revision12 = output.build.revision[0:12]
            output.build.gecko_revision = output.build.revision
            output.build.gecko_revision12 = output.build.revision[0:12]
        else:
            output.build.revision = coalesce(consume(props, "revision"), consume(props, "gecko_revision"), raw_release)
            output.build.revision12 = output.build.revision[0:12]

        consume(props, "commit_titles")

        # TODO: LOOKS LIKE A "release" LOOKUP CAN FILL IN THE REVISION
        #     if builddata['tree'].startswith('release-') and builddata['revision'] in [None, 'None']:
        #         try:
        #             url = 'https://hg.mozilla.org/releases/{tree}/json-rev/{release_tag}'.format(
        #                 tree=builddata['tree'].split('release-')[1],
        #                 release_tag=builddata['release']
        #             )
        #             response = requests.get(url)
        #             builddata['revision'] = response.json()['node']
        #         except Exception:
        #             # We cannot raise an exception due to a broken release rev for repacks
        #             # https://bugzilla.mozilla.org/show_bug.cgi?id=1219432#c1
        #             pass

        output.build.version = consume(props, "version")

        # BUILD ID AND DATE
        try:
            output.build.id = consume(props, "buildid")
            output.build.date = Date(consume(props, "builddate"))
            if not output.build.date:
                output.build.date = unicode2Date(output.build.id, "%Y%m%d%H%M%S")
        except Exception as _:
            output.build.id = "<error>"

        # LOCALE
        output.build.locale = coalesce(consume(props, "locale"), 'en-US')
        if props.locales:  # nightly repack build
            output.action.repack = coalesce(consume(props, "repack"), True)
            locales = consume(props, "locales")
            try:
                output.build.locales = json2value(locales).keys()
            except Exception:
                output.build.locales = locales.split(",")

        output.build.url = coalesce(consume(props, "packageUrl"), consume(props, "build_url"), consume(props, "buildurl"), consume(props, "fileURL"))
        output.run.logurl = coalesce(consume(props, "log_url"), consume(props, "logurl"))
        output.build.release = coalesce(raw_release, consume(props, "en_revision"), output.run.script.revision)
        output.run.machine.aws_id = consume(props, "aws_instance_id")
        output.run.machine.name = coalesce(consume(props, "subordinatename"), consume(props, "subordinate"), output.run.machine.aws_id)
        split_name = output.run.machine.name.split("-")
        if is_integer(split_name[-1]):
            # EXAMPLES
            # b-2008-ix-0106
            # t-w732-ix-047
            # bld-linux64-spot-013
            # panda-0150
            output.run.machine.pool = "-".join(split_name[:-1])
        output.run.machine.aws_type = consume(props, "aws_instance_type")

        # FILES
        blobber_files = consume(props, "blobber_files")
        try:
            if blobber_files:
                try:
                    files = json2value(blobber_files)
                except Exception:
                    files = blobber_files
                output.run.files = [
                    {"name": name, "url": url}
                    for name, url in files.items()
                ]
        except Exception as e:
            Log.error("Malformed `blobber_files` buildbot property: {{json}}", json=blobber_files, cause=e)

        # PRODUCT
        output.build.product = consume(props, "product").lower()
        if "xulrunner" in buildername:
            output.build.product = "xulrunner"

        # PLATFORM
        raw_platform = consume(props, "platform")
        if raw_platform:
            if "Code Coverage " in buildername:
                if raw_platform.endswith("-cc"):
                    raw_platform = raw_platform[:-3]
                elif raw_platform.endswith("-ccov"):
                    raw_platform = raw_platform[:-5]
                else:
                    Log.error("Not recognized: {{key}}\n{{data|json}}", key=buildername, data=data)
                buildername = buildername.replace("Code Coverage ", "")
                output.tags += ["code coverage"]

            if raw_platform not in KNOWN_PLATFORM:
                KNOWN_PLATFORM[raw_platform] = {"build": {"platform": "unknown"}}
                Log.error("Unknown platform {{platform}}\n{{data|json}}", platform=raw_platform, data=data)
            set_default(output, KNOWN_PLATFORM[raw_platform])

        # BRANCH
        output.build.branch = branch_name = coalesce(consume(props, "tree"), consume(props, "branch")).split("/")[-1]
        if not branch_name:
            Log.error("{{key|quote}} no 'branch' property", key=buildername)
        consume(props, "repo_path")

        if 'release' in buildername:
            output.tags += ['release']
        if 'devedition' in buildername.lower():
            output.tags += ['devedition']
        if buildername.endswith("nightly"):
            output.tags += ["nightly"]

        # PGO MARKUP
        pgo_build = unquote(consume(props, "pgo_build"))
        if pgo_build:
            output.build.type += ["pgo"]
        buildtype = consume(props, "buildtype")
        if buildtype:
            output.build.type += [buildtype]

        # DECODE buildername
        for b in BUILDER_NAMES:
            expected = strings.expand_template(b, {
                "branch": branch_name,
                "platform": raw_platform,
                "clean_platform": output.build.platform,
                "product": output.build.product,
                "vm": output.run.machine.vm,
                "step": output.action.step,
            })
            if buildername == expected:
                output.build.name = buildername
                output.properties, output.other = normalize_other(props)
                output.action.type = coalesce(output.action.type, "build")
                verify(output, data)
                return output

        if buildername.startswith("fuzzer"):
            output.build.product = "fuzzing"
            pass
        elif 'l10n' in buildername or 'repack' in buildername:
            output.action.repack = True
        elif buildername.startswith("jetpack-"):
            for t in BUILD_TYPES:
                if buildername.endswith("-" + t):
                    output.build.type += [t]

            match = re.match(strings.expand_template(
                "jetpack-(.*)-{{platform}}-{{type}}",
                {
                    "platform": raw_platform,
                    "type": unwraplist(list(set(listwrap(output.build.type))))
                }
            ), buildername)

            if not match:
                Log.error("Not recognized: {{key}} in \n{{data|json}}", key=buildername, data=data)

            if branch_name == "addon-sdk":
                output.build.branch = match.groups()[0]
        elif buildername.startswith("b2g_" + branch_name + "_") and endswith(buildername, ["nightly", "periodic", "dep", "build", "nonunified"]):
            output.build.trigger = trigger = endswith(buildername, ["nightly", "periodic", "dep", "build", "nonunified"])
            output.build.name = raw_platform = buildername[5 + len(branch_name):-(len(trigger) + 1)]
            output.action.type = "build"
            if raw_platform not in KNOWN_PLATFORM:
                if raw_platform not in self.unknown_platforms:
                    self.unknown_platforms += [raw_platform]
                    Log.error("Platform not recognized: {{platform}}\n{{data}}", platform=raw_platform, data=data)
                else:
                    return Data()  # ERROR INGNORED, ALREADY SENT
            set_default(output, KNOWN_PLATFORM[raw_platform])
        elif buildername.endswith("nightly"):
            output.build.trigger = "nightly"
            try:
                temp = buildername.split(" " + branch_name + " ")
                if len(temp) == 1:
                    raw_platform = temp[0]
                    build = ""
                else:
                    raw_platform, build = temp[0:2]

                output.build.name = buildername
                set_default(output, TEST_PLATFORMS[raw_platform])

                for t in BUILD_TYPES:
                    if t in build:
                        output.build.type += [t]
            except Exception:
                Log.error("Not recognized: {{key}} in \n{{data|json}}", key=buildername, data=data)

        elif buildername.endswith("build"):
            try:
                temp = buildername.split(" " + branch_name + " ")
                if len(temp) == 1:
                    raw_platform = temp[0]
                    build = ""
                else:
                    raw_platform, build = temp[0:2]

                output.build.name = raw_platform
                if raw_platform not in TEST_PLATFORMS:
                    if raw_platform not in self.unknown_platforms:
                        self.unknown_platforms += [raw_platform]
                        Log.error("Test platform not recognized: {{platform}}\n{{data}}", platform=raw_platform, data=data)
                    else:
                        return Data()  # ERROR INGNORED, ALREADY SENT
                set_default(output, TEST_PLATFORMS[raw_platform])
                output.action.type = "build"
            except Exception as e:
                raise Log.error("Not recognized: {{key}}\n{{data|json}}", key=buildername, data=data, cause=e)

            for t in BUILD_TYPES:
                if t in build:
                    output.build.type += [t]
        elif buildername.endswith("non-unified"):
            output.build.name = buildername
            raw_platform, build = buildername.split(" " + branch_name + " ")
            set_default(output, TEST_PLATFORMS[raw_platform])
        elif buildername.endswith("valgrind"):
            output.build.name = consume(props, "buildername")
            raw_platform, build = buildername.split(" " + branch_name + " ")
            set_default(output, TEST_PLATFORMS[raw_platform])
        else:
            # FORMAT: <platform> <branch> <test_mode> <test_name> <other>
            try:
                raw_platform, test = buildername.split(" " + branch_name + " ")
            except Exception:
                raise Log.error("No recognized branch name: {{key}}\n{{data}}", key=buildername, data=data)

            raw_test = consume(props, "test")
            raw_test = raw_test.replace("-pgo", "")  # NOT A TEST PROPERTY
            if raw_test and raw_test not in test:
                Log.error("do not know how to handle test discrepancy")

            output.build.name = raw_platform
            if raw_platform not in TEST_PLATFORMS:
                Log.error("Test Platform not recognized: {{platform}}\n{{data}}", platform=raw_platform, data=data)
                if raw_platform not in self.unknown_platforms:
                    self.unknown_platforms += [raw_platform]
                    Log.error("Test Platform not recognized: {{platform}}\n{{data}}", platform=raw_platform, data=data)
                else:
                    return Data()  # ERROR IGNORED, ALREADY SENT

            set_default(output, TEST_PLATFORMS[raw_platform])

            parsed = parse_test(test, output)
            if not parsed:
                Log.error("Test mode not recognized: {{key}}\n{{data|json}}", key=buildername, data=data)

        # OS MARKUP
        output.run.os = coalesce(output.run.os, consume(data, "os"))

        verify(output, data)
        output.properties, output.other = normalize_other(props)

        return output


def consume(props, key):
    output, props[key] = props[key], None
    return output


def verify(output, data):
    output.run.type = list(set(listwrap(output.run.type)))
    output.build.type = list(set(listwrap(output.build.type)))
    output.build.tags = list(set(output.build.tags))

    if "e10s" in data.properties.buildername.lower() and 'e10s' not in output.run.type:
        Log.error("Did not pickup e10s in\n{{data|json}}", data=data)
    if output.run.machine.os != None and output.run.machine.os not in ALLOWED_OS:
        ALLOWED_OS.append(output.run.machine.os)
        Log.error("Bad OS {{os}}\n{{data|json}}", os=output.run.machine.os, data=data)
    if "test" in output.action.type and output.build.platform not in ALLOWED_PLATFORMS:
        ALLOWED_PLATFORMS.append(output.build.platform)
        Log.error("Bad Platform {{platform}}\n{{data|json}}", platform=output.build.platform, data=data)
    if output.build.product not in ALLOWED_PRODUCTS:
        ALLOWED_PRODUCTS.append(output.build.product)
        Log.error("Bad Product {{product}}\n{{data|json}}", product=output.build.product, data=data)


def parse_test(test, output):
    # "test web-platform-tests-e10s-7"
    test = test.lower()

    # CHUNK NUMBER
    path = test.split("-")
    if is_integer(path[-1]):
        output.run.chunk = int(path[-1])
        test = "-".join(path[:-1])

    if "-e10s" in test:
        test = test.replace("-e10s", "")
        output.run.type += ["e10s"]

    if "-stylo" in test:
        test = test.replace("-stylo", "")
        output.run.type += ["stylo"]

    for m, d in test_modes.items():
        if test.startswith(m):
            set_default(output, d)
            output.run.suite = test[len(m):].strip()
            return True

    return False


def unquote(value):
    with suppress_exception:
        return ast.literal_eval(value)

    with suppress_exception:
        return json2value(value)

    return value


def normalize_other(other):
    """
    the buildbot properties are unlimited in their number of keys
    """
    known = Data()
    unknown = []
    for k, v in other.items():
        v = elasticsearch.scrub(v)
        if not v:
            continue

        if k in known_properties:
            known[k] = v
            continue

        if k not in unknown_properties:
            Log.alert("unknown properties: {{name|json}}", name=unknown_properties)
        unknown_properties[k] += 1
        if isinstance(v, (list, dict, Data)):
            unknown.append({"name": text(k), "value": value2json(v)})
        elif is_number(v):
            unknown.append({"name": text(k), "value": convert.value2number(v)})
        elif v in [True, "true", "True"]:
            unknown.append({"name": text(k), "value": True})
        elif v in [False, "false", "False"]:
            unknown.append({"name": text(k), "value": False})
        elif isinstance(v, text):
            unknown.append({"name": text(k), "value": v})
        else:
            Log.error("Do not know how to handle type {{type}}", type=v.__class__.__name__)

    known.uploadFiles = unquote(known.uploadFiles)
    known.partialInfo = unquote(known.partialInfo)
    return known, unknown


known_properties = {
    "appVersion",
    "aws_ami_id",
    "base_bundle_urls",
    "builddir",
    "comments",
    "got_revision",
    "main",
    "scheduler",
    "subordinatebuilddir",
    "stage_platform",
    "appName",
    "basedir",
    "builduid",
    "packageFilename",
    "sourcestamp",
    "symbolsUrl",
    "testsUrl",
    "testPackagesUrl",
    "uploadFiles"
}

unknown_properties=Data()


test_modes = {
    "test": {"action": {"type": "test"}},
    "debug test": {"build": {"type": ["debug"]}, "action": {"type": "test"}},
    "opt test": {"build": {"type": ["opt"]}, "action": {"type": "test"}},
    "pgo test": {"build": {"type": ["pgo"]}, "action": {"type": "test"}},
    "pgo talos": {"build": {"type": ["pgo"]}, "action": {"type": ["test", "talos"]}},
    "talos": {"action": {"type": ["test", "talos"]}}
}

BUILDER_NAMES = [
    '{{branch}}-{{product}}_{{platform}}_build',
    '{{branch}}-{{product}}_antivirus',
    '{{branch}}-{{product}}_almost_ready_for_release',
    '{{branch}}-{{product}}_beta_ready_for_beta-cdntest_testing',
    '{{branch}}-{{product}}_beta_ready_for_release',
    '{{branch}}-{{product}}_beta_start_uptake_monitoring',
    '{{branch}}-{{product}}_beta_updates',
    '{{branch}}-{{product}}_bouncer_submitter',
    '{{branch}}-{{product}}_checksums',
    '{{branch}}-{{product}}-esr_final_verification',
    '{{branch}}-{{product}}-esr_ready-for-esr-cdntest',
    '{{branch}}-{{product}}_esr_ready_for_esr-cdntest_testing',
    '{{branch}}-{{product}}_esr_ready_for_release',
    '{{branch}}-{{product}}_esr_start_uptake_monitoring',
    '{{branch}}-{{product}}_esr_updates',
    '{{branch}}-{{product}}_push_to_mirrors',
    '{{branch}}-{{product}}_postrelease',
    '{{branch}}-{{product}}_ready_for_release',
    '{{branch}}-{{product}}_ready_for_releasetest_testing',
    '{{branch}}-{{product}}_reset_schedulers',
    '{{branch}}-{{product}}_release_ready_for_release-cdntest_testing',
    '{{branch}}-{{product}}_release_ready_for_release',
    '{{branch}}-{{product}}_release_start_uptake_monitoring',
    '{{branch}}-{{product}}_release_updates',
    '{{branch}}-{{product}}_source',
    '{{branch}}-{{product}}_start_uptake_monitoring',
    '{{branch}}-{{product}}_tag_source',
    '{{branch}}-{{product}}_updates',

    '{{branch}}_{{platform}}',
    '{{branch}}-{{platform}}_build',
    '{{branch}}-{{platform}}_update_verify_{{step}}',
    '{{branch}}-{{platform}}_update_verify_beta_{{step}}',
    '{{branch}}-{{platform}}_update_verify_esr_{{step}}',
    '{{branch}}-{{platform}}_update_verify_release_{{step}}',
    '{{branch}}-{{platform}}_ui_update_verify_beta_{{step}}',

    '{{branch}}-antivirus',
    '{{branch}}-beta_final_verification',
    '{{branch}}-check_permissions',
    '{{branch}}-esr_final_verification',
    '{{branch}}-final_verification',
    '{{branch}} hg bundle',
    '{{branch}}-release_final_verification',
    '{{branch}}-update_shipping',
    '{{branch}}-update_shipping_beta',
    '{{branch}}-update_shipping_esr',
    '{{branch}}-update_shipping_release',
    '{{branch}}-xr_postrelease',
    '{{platform}}_{{branch}}_dep',
    '{{platform}} {{branch}} periodic file update',
    'Linux x86-64 {{branch}} periodic file update',  # THE platform DOES NOT MATCH
    'linux64-br-haz_{{branch}}_dep',
    'release-{{branch}}_{{product}}_{{platform}}_update_verify',
    'release-{{branch}}_{{product}}_bncr_sub',
    'release-{{branch}}-{{product}}_bncr_sub',
    'release-{{branch}}-{{product}}_bouncer_aliases',
    'release-{{branch}}-{{product}}_chcksms',
    'release-{{branch}}-{{product}}_publish_balrog',
    'release-{{branch}}-{{product}}_mark_as_shipped',
    'release-{{branch}}-{{product}}_schedule_publishing_in_balrog',
    'release-{{branch}}-{{product}}_updates',
    'release-{{branch}}-{{product}}_version_bump',
    'release-{{branch}}-{{product}}_uptake_monitoring',
    '{{vm}}_{{branch}}_{{clean_platform}} nightly',
    '{{vm}}_{{branch}}_{{clean_platform}} build'
]

BUILD_TYPES = [
    "asan",   # ADDRESS SANITIZER
    "debug",  # FOR DEBUGGING
    "mulet",  # COMMON FRAMEWORK FOR b2g and Firefox
    "opt",    # OPTIMIZED
    "pgo",    # PROFILE GUIDED OPTIMIZATIONS
    "tsan",   # THREAD SANITIZER
    "l10n",   # INTERNATIONALIZATION
    "leak test",
    "static analysis",
    "ccov",   # C/C++ CODE COVERAGE
    "ubsan",  #
    "jsdcov"  # JAVASCRIPT CODE COVERAGE
]

TEST_PLATFORMS = {
    "Android 2.3": {"run": {"machine": {"os": "android 2.3"}}, "build": {"platform": "android"}},
    "Android 2.3 Armv6": {"run": {"machine": {"os": "android 2.3", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android 2.3 Armv6 Emulator": {"run": {"machine": {"os": "android 2.3", "type": "emulator armv6"}}, "build": {"platform": "android"}},
    "Android 2.3 Emulator": {"run": {"machine": {"os": "android 2.3", "type": "emulator"}}, "build": {"platform": "android"}},
    "Android 2.3 Debug": {"run": {"machine": {"os": "android 2.3"}}, "build": {"platform": "android", "type": ["debug"]}},
    "Android 4.0 armv7 API 10+": {"run": {"machine": {"os": "android 4.0", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android 4.0 armv7 API 11+": {"run": {"machine": {"os": "android 4.0", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android 4.0 Panda": {"run": {"machine": {"os": "android 4.0", "type": "panda"}}, "build": {"platform": "android"}},
    "Android 4.2 x86": {"run": {"machine": {"os": "android 4.2", "type": "emulator x86"}}, "build": {"platform": "android"}},
    "Android 4.2 x86 Emulator": {"run": {"machine": {"os": "android 4.2", "type": "emulator x86"}}, "build": {"platform": "android"}},
    "Android 4.3 armv7 API 11+": {"run": {"machine": {"os": "android 4.3", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android 4.3 armv7 API 15+": {"run": {"machine": {"os": "android 4.3", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android 4.3 Emulator": {"run": {"machine": {"os": "android 4.3", "type": "emulator"}}, "build": {"platform": "android"}},
    "Android armv7 API 9": {"run": {"machine": {"os": "android 2.3", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android armv7 API 10+": {"run": {"machine": {"os": "android 2.3.3", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android armv7 API 11+": {"run": {"machine": {"os": "android 3.0", "type": "arm7"}}, "build": {"platform": "android"}},
    "Android armv7 API 15+": {"run": {"machine": {"os": "android 4.0.3", "type": "arm7"}}, "build": {"platform": "android"}},
    "b2g_emulator-kk_vm": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "flame"}},
    "b2g_emulator_vm": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g"}},
    "b2g_ubuntu32_vm": {"run": {"machine": {"os": "b2g", "type": "emulator32"}}, "build": {"platform": "b2g"}},
    "b2g_ubuntu64_vm": {"run": {"machine": {"os": "b2g", "type": "emulator64"}}, "build": {"platform": "b2g"}},
    "b2g_macosx64": {"run": {"machine": {"os": "b2g", "type": "emulator64"}}, "build": {"platform": "b2g"}},
    "Linux": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux32"}},
    "Linux x86-64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64"}},
    "Linux x86-64 Mulet": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64", "type": ["mulet"]}},
    "Linux x86-64 add-on-devel": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64", "type": ["add-on"]}},
    "OS X 10.7": {"run": {"machine": {"os": "lion 10.7"}}, "build": {"platform": "macosx64"}},
    "OS X 10.7 64-bit": {"run": {"machine": {"os": "lion 10.7"}}, "build": {"platform": "macosx64"}},
    "OS X 10.7  add-on-devel": {"run": {"machine": {"os": "lion 10.7"}}, "build": {"platform": "macosx64", "type": ["add-on"]}},
    "OS X Mulet": {"run": {"machine": {"os": "macosx"}}, "build": {"platform": "macosx", "type": ["mulet"]}},
    "Rev4 MacOSX Snow Leopard 10.6": {"run": {"machine": {"os": "snowleopard 10.6"}}, "build": {"platform": "macosx64"}},
    "Rev5 MacOSX Mountain Lion 10.8": {"run": {"machine": {"os": "mountain lion 10.8"}}, "build": {"platform": "macosx64"}},
    "Rev5 MacOSX Yosemite 10.10": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64"}},
    "Rev5 MacOSX Yosemite 10.10.5": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64"}},
    "Rev7 MacOSX Yosemite 10.10.5": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64"}},
    "Rev7 MacOSX Yosemite 10.10.5 DevEdition": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64", "type": ["devedition"]}},
    "Ubuntu ASAN VM large 12.04 x64": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64", "type": ["asan"]}},
    "Ubuntu ASAN VM 12.04 x64": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64", "type": ["asan"]}},
    "Ubuntu TSAN VM 12.04 x64": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64", "type": ["tsan"]}},
    "Ubuntu TSAN VM large 12.04 x64": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64", "type": ["tsan"]}},
    "Ubuntu HW 12.04": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux32"}},
    "Ubuntu HW 12.04 x64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64"}},
    "Ubuntu HW 12.04 x64 qr": {"run": {"type": ["qr"], "machine": {"os": "ubuntu"}}, "build": {"platform": "linux64"}},
    "Ubuntu HW 12.04 x64 stylo": {"run": {"machine": {"os": "ubuntu"}, "type":["stylo"]}, "build": {"platform": "linux64"}},
    "Ubuntu HW 12.04 x64 stylo-sequential": {"run": {"machine": {"os": "ubuntu"}, "type":["stylo"]}, "build": {"platform": "linux64", "type": ["sequential"]}},
    "Ubuntu HW 12.04 x64 devedition": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64", "type": ["devedition"]}},
    "Ubuntu VM 12.04": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux32"}},
    "Ubuntu VM 12.04 x64": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64"}},
    "Ubuntu VM large 12.04 x64": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64"}},
    "Ubuntu VM 12.04 x64 Mulet": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64", "type": ["mulet"]}},
    "Windows XP 32-bit": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win32"}},
    "Windows 7 32-bit": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win32"}},
    "Windows 7 32-bit DevEdition": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win32", "type": ["devedition"]}},
    "Windows 7 32-bit stylo-disabled": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win32", "type": ["stylo-disabled"]}},
    "Windows 7 VM 32-bit": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win32"}},
    "Windows 7 VM 32-bit DevEdition": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win32", "type": ["devedition"]}},
    "Windows 7 VM-GFX 32-bit": {"run": {"machine": {"os": "win7"}, "type": ["gfx"]}, "build": {"platform": "win32"}},
    "Windows 7 VM-GFX 32-bit DevEdition": {"run": {"machine": {"os": "win7"}, "type": ["gfx"]}, "build": {"platform": "win32", "type": ["devedition"]}},
    "Windows 8 64-bit": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64"}},
    "Windows 8 64-bit DevEdition": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64", "type": ["devedition"]}},
    "Windows 10 64-bit": {"run": {"machine": {"os": "win10"}}, "build": {"platform": "win64"}},
    "Windows 10 64-bit DevEdition": {"run": {"machine": {"os": "win10"}}, "build": {"platform": "win64", "type": ["devedition"]}},
    "Windows 10 64-bit stylo-disabled": {"run": {"machine": {"os": "win10"}}, "build": {"platform": "win64", "type": ["stylo-disabled"]}},
    "WINNT 5.2": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win64"}},
    "WINNT 5.2 add-on-devel": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win64", "type": ["add-on"]}},
    "WINNT 6.1 x86-64": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win64"}},
    "WINNT 6.1 x86-64 add-on-devel": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win64", "type": ["add-on"]}},
    "WINNT 6.2": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64"}},
    "Win32 Mulet": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win32", "type": ["mulet"]}},
}

STATUS_CODES = {
    0: "success",
    1: "warnings",
    2: "failure",
    3: "skipped",
    4: "exception",
    5: "retry",
    6: "cancelled",
    "0": "success",
    "1": "warnings",
    "2": "failure",
    "3": "skipped",
    "4": "exception",
    "5": "retry",
    "6": "cancelled",
    None: None,
    "success (0)": "success",
    "warnings (1)": "warnings",
    "failure (2)": "failure",
    "skipped (3)": "skipped",
    "exception (4)": "exception",
    "retry (5)": "retry",
    "cancelled (6)": "cancelled"
}

RATIO_PATTERN = re.compile(r"(\d+/\d+)")

ALLOWED_OS = [
    "android 2.3",
    "android 3.0",
    "android 4.0",
    "android 4.2",
    "android 4.3",
    "android 4.0.3",
    "b2g",
    "lion 10.7",
    "macosx",
    "mountain lion 10.10",
    "snowleopard 10.6",
    "ubuntu",
    "winxp",
    "win7",
    "win8",
    "win10",
    "yosemite 10.10"
]

KNOWN_PLATFORM = {
    "android": {"build": {"platform": "android"}},
    "android-armv6": {"run": {"machine": {"type": "arm7"}}, "build": {"platform": "android"}},
    "android-debug": {"build": {"platform": "android", "type": ["debug"]}},
    "android-api-9": {"run": {"machine": {"os": "android 2.3"}}, "build": {"platform": "android"}},
    "android-api-9-debug": {"run": {"machine": {"os": "android 2.3"}}, "build": {"platform": "android", "type": ["debug"]}},
    "android-api-11": {"run": {"machine": {"os": "android 3.0"}}, "build": {"platform": "android"}},
    "android-api-11-debug": {"run": {"machine": {"os": "android 3.0"}}, "build": {"platform": "android", "type": ["debug"]}},
    "android-api-15": {"run": {"machine": {"os": "android 4.0.3"}}, "build": {"platform": "android"}},
    "android-api-15-debug": {"run": {"machine": {"os": "android 4.0.3"}}, "build": {"platform": "android", "type": ["debug"]}},
    "android-x86": {"run": {"type": "emulator"}, "build": {"platform": "android"}},
    "panda_android": {"run": {"type": "panda"}, "build": {"platform": "android"}},
    "b2g": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "b2g"}},
    "dolphin": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "dolphin"}},
    "dolphin_eng": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "dolphin"}},
    "dolphin-512": {"run": {"machine": {"os": "b2g", "memory": 512}}, "build": {"platform": "dolphin"}},
    "dolphin-512_eng": {"run": {"machine": {"os": "b2g", "memory": 512}}, "build": {"platform": "dolphin"}},
    "emulator": {"run": {"machine": {"type": "emulator"}}},
    "emulator-debug": {"run": {"machine": {"type": "emulator"}}, "build": {"type": ["debug"]}},
    "emulator-jb": {"run": {"machine": {"type": "emulator"}}, "build": {}},
    "emulator-jb-debug": {"run": {"machine": {"type": "emulator"}}, "build": {"type": ["debug"]}},
    "emulator-kk": {"run": {"machine": {"type": "emulator"}}, "build": {"platform": "flame"}},
    "emulator-kk-debug": {"run": {"machine": {"type": "emulator"}}, "build": {"platform": "flame", "type": ["debug"]}},
    "emulator-l": {"run": {"machine": {"type": "emulator"}}},
    "emulator-l-debug": {"run": {"machine": {"type": "emulator"}}, "build": {"type": ["debug"]}},
    "flame": {"build": {"platform": "flame"}},
    "flame_eng": {"build": {"platform": "flame"}},
    "flame-kk": {"build": {"platform": "flame"}},
    "flame-kk_eng": {"build": {"platform": "flame"}},
    "flame-kk_eng-debug": {"build": {"platform": "flame", "type": ["debug"]}},
    "hamachi": {"build": {"platform": "hamachi"}},
    "hamachi_eng": {"build": {"platform": "hamachi"}},
    "helix": {"build": {"platform": "helix"}},
    "l10n": {"action": {"type": "l10n"}},
    "linux": {"build": {"platform": "linux32"}},
    "linux-debug": {"build": {"platform": "linux32", "type": ["debug"]}},
    "linux32_gecko": {"build": {"platform": "linux32"}},
    "linux32_gecko-debug": {"build": {"platform": "linux32", "type": ["debug"]}},
    "linux32_gecko_localizer": {},
    "linux64": {"build": {"platform": "linux64"}},
    "linux64-add-on-devel": {"build": {"platform": "linux64", "type": ["add-on"]}},
    "linux64-asan": {"build": {"platform": "linux64", "type": ["asan"]}},
    "linux64-asan-debug": {"build": {"platform": "linux64", "type": ["asan", "debug"]}},
    "linux64-b2g-haz": {"action": {"type": "hazard build"}, "run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g"}},
    "linux64-br-haz": {"action": {"type": "hazard build"}, "build": {"platform": "linux64"}},
    "linux64-debug": {"build": {"platform": "linux64", "type": ["debug"]}},
    "linux64_gecko": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g"}},
    "linux64_gecko-debug": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g", "type": ["debug"]}},
    "linux64_gecko_localizer": {},
    "linux64_graphene": {"run": {"machine": {"vm": "graphene"}}, "build": {"platform": "linux64"}},
    "linux64_horizon": {"run": {"machine": {"vm": "horizon"}}, "build": {"platform": "linux64"}},
    "linux64-tsan": {"build": {"platform": "linux64", "type": ["tsan"]}},
    "linux64-mulet": {"build": {"platform": "linux64", "type": ["mulet"]}},
    "linux64-st-an-debug": {"build": {"platform": "linux64", "type": ["debug", "static analysis"]}},
    "linux64-sh-haz": {"action": {"type": "hazard build"}, "build": {"platform": "linux64"}},
    "macosx64": {"build": {"platform": "macosx64"}},
    "macosx64-add-on-devel": {"build": {"platform": "macosx64", "type": ["add-on"]}},
    "macosx64-debug": {"build": {"platform": "macosx64", "type": ["debug"]}},
    "macosx64-devedition":  {"build": {"platform": "macosx64", "type":["devedition"]}},
    "macosx64_gecko": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g"}},
    "macosx64_gecko-debug": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g", "type": ["debug"]}},
    "macosx64_gecko_localizer": {},
    "macosx64_graphene": {"run": {"machine": {"vm": "graphene"}}, "build": {"platform": "macosx64"}},
    "macosx64_horizon": {"run": {"machine": {"vm": "horizon"}}, "build": {"platform": "macosx64"}},
    "macosx64-lion": {"run": {"machine": {"os": "lion 10.7"}}, "build": {"platform": "macosx64"}},
    "macosx64-mulet": {"build": {"platform": "macosx64", "type": ["mulet"]}},
    "macosx64-st-an-debug": {"build": {"platform": "macosx64", "type": ["debug", "static analysis"]}},
    "mock": {"build": {"platform": "mock"}},
    "mountainlion": {"run": {"machine": {"os": "mountain lion 10.10"}}, "build": {"platform": "macosx64"}},
    "nexus-4": {"build": {"platform": "nexus4"}},
    "nexus-4_eng": {"build": {"platform": "nexus4"}},
    "nexus-5-l": {"build": {"platform": "nexus5"}},
    "nexus-5-l_eng": {"build": {"platform": "nexus5"}},
    "OS X 10.7  add-on-devel": {"build": {"platform": "macosx64", "type": ["add-on"]}},
    "source": {},
    "snowleopard": {"run": {"machine": {"os": "snowleopard 10.6"}}, "build": {"platform": "macosx64"}},
    "ubuntu32_hw": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux32"}},
    "ubuntu64-asan_vm": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64", "type": ["asan"]}},
    "ubuntu64_hw": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64"}},
    "ubuntu64_hw_devedition": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64", "type": ["devedition"]}},
    "ubuntu64_hw_qr": {"run": {"type": ["qr"], "machine": {"os": "ubuntu"}}, "build": {"platform": "linux64"}},
    "ubuntu64_hw_stylo": {"run": {"machine": {"os": "ubuntu"}, "type": ["stylo"]}, "build": {"platform": "linux64"}},
    "ubuntu64_hw_styloseq": {"run": {"machine": {"os": "ubuntu"}, "type": ["stylo"]}, "build": {"platform": "linux64", "type": ["sequential"]}},
    "ubuntu32_vm": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux32"}},
    "ubuntu64_vm_lnx_large": {"run": {"machine": {"os": "ubuntu", "type": "vm"}}, "build": {"platform": "linux64"}},
    "wasabi": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "wasabi"}},
    "win32": {"build": {"platform": "win32"}},
    "win32-add-on-devel": {"build": {"platform": "win32", "type": ["add-on"]}},
    "win32-debug": {"build": {"platform": "win32", "type": ["debug"]}},
    "win32-devedition": {"build": {"platform": "win32", "type": ["debug", "devedition"]}},
    "win32-mulet": {"build": {"platform": "win32", "type": ["mulet"]}},
    "win32_gecko": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g"}},
    "win32_gecko-debug": {"run": {"machine": {"os": "b2g", "type": "emulator"}}, "build": {"platform": "b2g", "type": ["debug"]}},
    "win32_gecko_localizer": {},
    "win32-st-an-debug": {"build": {"platform": "win32", "type": ["debug", "static analysis"]}},
    "win32-stylo-disabled": {"build": {"platform": "win32", "type": ["stylo-disabled"]}},
    "win64": {"build": {"platform": "win64"}},
    "win64-add-on-devel": {"build": {"platform": "win64", "type": ["add-on"]}},
    "win64-debug": {"build": {"platform": "win64", "type": ["debug"]}},
    "win64-devedition":  {"build": {"platform": "win64", "type":["devedition"]}},
    "win64_graphene": {"run": {"machine": {"vm": "graphene"}}, "build": {"platform": "win64", "type":["devedition"]}},
    "win64_horizon": {"run": {"machine": {"vm": "horizon"}}, "build": {"platform": "win64"}},
    "win64-rev2": {"build": {"platform": "win64"}},
    "win64-stylo-disabled": {"build": {"platform": "win64", "type": ["stylo-disabled"]}},
    "win7-ix": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win64"}},
    "win7_ix": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win64"}},
    "win7_ix_devedition": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win64"}},
    "win8": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64"}},
    "win8_64": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64"}},
    "win8_64_devedition": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64", "type": ["devedition"]}},
    "win10_64": {"run": {"machine": {"os": "win10"}}, "build": {"platform": "win64"}},
    "win10_64_devedition": {"run": {"machine": {"os": "win10"}}, "build": {"platform": "win64", "type": ["devedition"]}},
    "xp-ix": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win32"}},
    "xp_ix": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win32"}},
    "yosemite": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64"}},
    "yosemite_r7": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64"}},
    "yosemite_r7_devedition": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64", "type":["devedition"]}}
}


ALLOWED_PLATFORMS = [
    "android",
    "b2g",
    "flame",
    "linux32",
    "linux64",
    "l10n",  # FOR repack
    "macosx32",
    "macosx64",
    "mock",  # FOR fuzzing
    "win32",
    "win64"
]


ALLOWED_PRODUCTS = [
    None,
    "b2g",
    "devedition",
    "fennec",
    "firefox",
    "fuzzing",
    "jetpack",
    "mobile",
    "spidermonkey",
    "thunderbird",
    "xulrunner"
]



def endswith(key, options):
    for o in options:
        if key.endswith(o):
            return o
