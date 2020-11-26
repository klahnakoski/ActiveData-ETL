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

import datetime
from math import sqrt

import mo_math
from activedata_etl.transforms import TRY_AGAIN_LATER
from activedata_etl.transforms.pulse_block_to_es import transform_buildbot
from jx_python import jx
from mo_dots import (
    literal_field,
    Data,
    FlatList,
    coalesce,
    unwrap,
    set_default,
    listwrap,
    unwraplist,
    wrap,
)
from mo_future import text
from mo_json import json2value
from mo_logs import Log
from mo_math import MIN, MAX
from mo_math.stats import ZeroMoment2Stats, ZeroMoment
from mo_threads import Lock
from mo_times.dates import Date
from pyLibrary.env import git

DEBUG = True
ARRAY_TOO_BIG = 1000
NOW = datetime.datetime.utcnow()
TOO_OLD = NOW - datetime.timedelta(days=30)
PUSHLOG_TOO_OLD = NOW - datetime.timedelta(days=7)


repo = None
locker = Lock()
unknown_branches = set()


def process(source_key, source, destination, resources, please_stop=None):
    global repo
    if repo is None:
        repo = unwrap(resources.hg)

    lines = source.read_lines()

    records = []
    i = 0
    for line in lines:
        perfherder_record = None
        try:
            perfherder_record = json2value(line)
            if not perfherder_record:
                continue
            etl_source = perfherder_record.etl

            if perfherder_record.suites:
                Log.error(
                    "Should not happen, perfherder storage iterates through the suites"
                )

            if perfherder_record.pulse:
                metadata = transform_buildbot(
                    source_key, perfherder_record.pulse, resources
                )
                perfherder_record.pulse = None
            elif perfherder_record.task or perfherder_record.is_empty:
                metadata, perfherder_record.task = perfherder_record.task, None
            else:
                Log.warning(
                    "Expecting some task/job information. key={{key}}",
                    key=perfherder_record._id,
                )
                continue

            if not isinstance(metadata.run.suite, text):
                metadata.run.suite = metadata.run.suite.fullname

            perf_records = transform(source_key, perfherder_record, metadata, resources)
            for p in perf_records:
                p["etl"] = {
                    "id": i,
                    "source": etl_source,
                    "type": "join",
                    "revision": git.get_revision(),
                    "timestamp": Date.now(),
                }
                key = source_key + "." + text(i)
                records.append({"id": key, "value": p})
                i += 1
        except Exception as e:
            if TRY_AGAIN_LATER:
                Log.error("Did not finish processing {{key}}", key=source_key, cause=e)

            Log.warning(
                "Problem with pulse payload {{pulse|json}}",
                pulse=perfherder_record,
                cause=e,
            )

    if not records:
        Log.warning("No perfherder records are found in {{key}}", key=source_key)

    try:
        destination.extend(records, overwrite=True)
        return [source_key]
    except Exception as e:
        Log.error(
            "Could not add {{num}} documents when processing key {{key}}",
            key=source_key,
            num=len(records),
            cause=e,
        )


# CONVERT THE TESTS (WHICH ARE IN A dict) TO MANY RECORDS WITH ONE result EACH
def transform(source_key, perfherder, metadata, resources):

    def scrub_subtest(subtest):
        unknown = subtest.keys()-set(KNOWN_SUBTEST_PROPERTIES.keys())
        if unknown:
            Log.warning("unknown properties {{props}} in subtest while processing {{key}}", props=unknown, key=source_key)
            KNOWN_SUBTEST_PROPERTIES[unknown] = unknown

        output = Data()
        for k, v in subtest.items():
            a = KNOWN_SUBTEST_PROPERTIES[k]
            output[a] = coalesce(output[a], v)
        return output

    if perfherder.is_empty:
        return [metadata]

    try:
        framework_name = perfherder.framework.name
        suite_name = coalesce(
            perfherder.testrun.suite, perfherder.name, metadata.run.suite
        )
        if not suite_name:
            if perfherder.is_empty:
                # RETURN A PLACEHOLDER
                metadata.run.timestamp = coalesce(
                    perfherder.testrun.date,
                    metadata.run.timestamp,
                    metadata.action.timestamp,
                    metadata.action.start_time,
                )
                return [metadata]
            else:
                Log.error("Can not process: no suite name is found")

        for option in KNOWN_PERFHERDER_OPTIONS:
            if suite_name.find("-" + option) >= 0:
                if option == "coverage":
                    pass  # coverage matches "jsdcov" and many others, do not bother sending warnings if not found
                elif (
                    option
                    not in listwrap(metadata.run.type) + listwrap(metadata.build.type)
                    and framework_name != "job_resource_usage"
                ):
                    Log.warning(
                        "While processing {{uid}}, found {{option|quote}} in {{name|quote}} but not in run.type (run.type={{metadata.run.type}}, build.type={{metadata.build.type}})",
                        uid=source_key,
                        metadata=metadata,
                        name=suite_name,
                        perfherder=perfherder,
                        option=option,
                    )
                    metadata.run.type = unwraplist(
                        listwrap(metadata.run.type) + [option]
                    )
                suite_name = suite_name.replace("-" + option, "")

        # RECOGNIZE SUITE
        for s in KNOWN_PERFHERDER_TESTS:
            if suite_name == s:
                break
            elif suite_name.startswith(s) and framework_name != "job_resource_usage":
                Log.warning(
                    "While processing {{uid}}, removing suite suffix of {{suffix|quote}} for {{suite}} in framework {{framework}}",
                    uid=source_key,
                    suffix=suite_name[len(s) : :],
                    suite=suite_name,
                    framework=framework_name,
                )
                suite_name = s
                break
            elif suite_name.startswith("remote-" + s):
                suite_name = "remote-" + s
                break
        else:
            if suite_name.startswith("raptor-") and suite_name.endswith(
                tuple(RAPTOR_BROWSERS)
            ):  # ACCEPT ALL RAPTOR NAMES,
                pass
            elif not perfherder.is_empty and framework_name not in ("raptor", "job_resource_usage", "browsertime"):
                Log.warning(
                    "While processing {{uid}}, found unknown perfherder suite by name of {{name|quote}} (run.type={{metadata.run.type}}, build.type={{metadata.build.type}})",
                    uid=source_key,
                    metadata=metadata,
                    name=suite_name,
                    perfherder=perfherder,
                )
                KNOWN_PERFHERDER_TESTS.append(suite_name)

        # UPDATE metadata PROPERTIES TO BETTER VALUES
        metadata.run.timestamp = coalesce(
            perfherder.testrun.date,
            metadata.run.timestamp,
            metadata.action.timestamp,
            metadata.action.start_time,
        )
        metadata.result.suite = metadata.run.suite = suite_name
        metadata.result.framework = metadata.run.framework = perfherder.framework
        metadata.result.application = perfherder.application
        metadata.result.extraOptions = perfherder.extraOptions
        metadata.result.hgVersion = perfherder.hgVersion

        mainthread_transform(perfherder.results_aux)
        mainthread_transform(perfherder.results_xperf)

        new_records = FlatList()

        # RECORD THE UNKNOWN PART OF THE TEST RESULTS
        unknown_props = perfherder.keys() - KNOWN_PERFHERDER_PROPERTIES
        if unknown_props:
            # IF YOU ARE HERE, BE SURE THESE PROPERTIES ARE PUT INT THE result OBJECT (below)
            Log.warning("unknown perfherder property {{unknown_props}} while processing key=={{key}}", key=source_key, unknown_props=unknown_props)

        result_template = {
            "alert": {
                "enabled": perfherder.shouldAlert,
                "threshold": perfherder.alertThreshold,
                "change_type": perfherder.alertChangeType,
            },
            "type": perfherder.type,
            "server_url": perfherder.serverUrl,
            "tags": perfherder.tags
        }

        total = FlatList()

        if perfherder.subtests:
            if suite_name in ["dromaeo_css", "dromaeo_dom"]:
                # dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                for i, subtest in enumerate(perfherder.subtests):
                    subtest_template = scrub_subtest(subtest)
                    for g, sub_replicates in jx.chunk(subtest.replicates, size=5):
                        new_record = set_default(
                            {
                                "result": set_default(
                                    stats(
                                        source_key,
                                        sub_replicates,
                                        subtest.name,
                                        suite_name,
                                    ),
                                    {
                                        "test": text(subtest.name)
                                        + "."
                                        + text(g),
                                        "ordering": i,
                                    },
                                    subtest_template,
                                    result_template,
                                )
                            },
                            metadata,
                        )
                        new_records.append(new_record)
                        total.append(new_record.result.stats)
            else:
                for i, subtest in enumerate(perfherder.subtests):
                    subtest_template = scrub_subtest(subtest)
                    samples = coalesce(subtest.replicates, [subtest.value])
                    new_record = set_default(
                        {
                            "result": set_default(
                                stats(source_key, samples, subtest.name, suite_name),
                                {
                                    "ordering": i,
                                    "value": samples[0] if len(samples) == 1 else None,
                                },
                                subtest_template,
                                result_template,
                            )
                        },
                        metadata,
                    )
                    new_records.append(new_record)
                    total.append(new_record.result.stats)

        elif perfherder.results:
            # RECORD TEST RESULTS
            if suite_name in ["dromaeo_css", "dromaeo_dom"]:
                # dromaeo IS SPECIAL, REPLICATES ARE IN SETS OF FIVE
                # RECORD ALL RESULTS
                for i, (test_name, replicates) in enumerate(perfherder.results.items()):
                    for g, sub_replicates in jx.chunk(replicates, size=5):
                        new_record = set_default(
                            {
                                "result": set_default(
                                    stats(
                                        source_key,
                                        sub_replicates,
                                        test_name,
                                        suite_name,
                                    ),
                                    {
                                        "test": text(test_name) + "." + text(g),
                                        "ordering": i,
                                        "value": replicates[0] if len(sub_replicates) == 1 else None,
                                    },
                                    result_template,
                                )
                            },
                            metadata,
                        )
                        new_records.append(new_record)
                        total.append(new_record.result.stats)
            else:
                for i, (test_name, replicates) in enumerate(perfherder.results.items()):
                    new_record = set_default(
                        {
                            "result": set_default(
                                stats(source_key, replicates, test_name, suite_name),
                                {
                                    "test": test_name,
                                    "ordering": i,
                                    "value": replicates[0] if len(replicates) == 1 else None,
                                },
                                result_template,
                            )
                        },
                        metadata,
                    )
                    new_records.append(new_record)
                    total.append(new_record.result.stats)
        elif (perfherder.value != None):
            # SUITE CAN HAVE A SINGLE VALUE, AND NO SUB-TESTS
            new_record = set_default(
                {
                    "result": set_default(
                        stats(source_key, [perfherder.value], None, suite_name),
                        {"value": perfherder.value},
                        result_template,
                    )
                },
                metadata,
            )
            new_records.append(new_record)
            total.append(new_record.result.stats)
        elif perfherder.is_empty:
            metadata.run.result.is_empty = True
            new_records.append(metadata)
            pass
        else:
            new_records.append(metadata)
            if suite_name == "sessionrestore_no_auto_restore":
                # OFTEN HAS NOTHING
                Log.note(
                    "While processing {{uid}}, no `results` or `subtests` found in {{name|quote}}",
                    uid=source_key,
                    name=suite_name,
                )
            else:
                Log.warning(
                    "While processing {{uid}}, no `results` or `subtests` found in {{name|quote}}",
                    uid=source_key,
                    name=suite_name,
                )

        # ADD RECORD FOR GEOMETRIC MEAN SUMMARY
        metadata.run.stats = geo_mean(total)
        Log.note(
            "Done {{uid}}, processed {{framework|upper}} :: {{name}}, transformed {{num}} records",
            uid=source_key,
            framework=framework_name,
            name=suite_name,
            num=len(new_records),
        )
        return new_records
    except Exception as e:
        Log.error("Transformation failure on id={{uid}}", {"uid": source_key}, e)


def mainthread_transform(r):
    if r == None:
        return None

    output = Data()

    for i in r.mainthread_readbytes:
        output[literal_field(i[1])].name = i[1]
        output[literal_field(i[1])].readbytes = i[0]
    r.mainthread_readbytes = None

    for i in r.mainthread_writebytes:
        output[literal_field(i[1])].name = i[1]
        output[literal_field(i[1])].writebytes = i[0]
    r.mainthread_writebytes = None

    for i in r.mainthread_readcount:
        output[literal_field(i[1])].name = i[1]
        output[literal_field(i[1])].readcount = i[0]
    r.mainthread_readcount = None

    for i in r.mainthread_writecount:
        output[literal_field(i[1])].name = i[1]
        output[literal_field(i[1])].writecount = i[0]
    r.mainthread_writecount = None

    r.mainthread = output.values()

def stats(source_key, given_values, test, suite):
    """
    RETURN dict WITH
    source_key - NAME OF THE SOURCE (FOR LOGGING ERRORS)
    stats - LOTS OF AGGREGATES
    samples - LIST OF VALUES USED IN AGGREGATE
    rejects - LIST OF VALUES NOT USED IN AGGREGATE
    """
    try:
        if given_values == None:
            return None

        rejects = unwraplist(
            [
                text(v)
                for v in given_values
                if mo_math.is_nan(v) or not mo_math.is_finite(v)
            ]
        )
        clean_values = wrap(
            [float(v) for v in given_values if not mo_math.is_nan(v) and mo_math.is_finite(v)]
        )

        z = ZeroMoment.new_instance(clean_values)
        s = wrap(z.__data__())
        for k, v in z.__data__().items():
            s[k] = v
        for k, v in ZeroMoment2Stats(z).items():
            s[k] = v
        s.max = MAX(clean_values)
        s.min = MIN(clean_values)
        s.median = mo_math.stats.median(clean_values, simple=False)
        s.last = clean_values.last()
        s.first = clean_values[0]
        if mo_math.is_number(s.variance) and not mo_math.is_nan(s.variance):
            s.std = sqrt(s.variance)

        good_excuse = [
            not rejects,
            suite in ["basic_compositor_video"],
            test in ["sessionrestore_no_auto_restore"],
        ]

        if not any(good_excuse):
            Log.warning(
                "{{test}} in suite {{suite}} in {{key}} has rejects {{samples|json}}",
                test=test,
                suite=suite,
                key=source_key,
                samples=given_values,
            )

        return {"stats": s, "samples": clean_values, "rejects": rejects}
    except Exception as e:
        Log.warning("can not reduce series to moments", e)
        return {}


def geo_mean(values):
    """
    GIVEN AN ARRAY OF dicts, CALC THE GEO-MEAN ON EACH ATTRIBUTE
    """
    agg = Data()
    for d in values:
        for k, v in d.items():
            if v != 0:
                acc = agg[k]
                if acc == None:
                    acc = agg[k] = ZeroMoment.new_instance()
                acc += mo_math.log(mo_math.abs(v))
    return {k: mo_math.exp(v.stats.mean) for k, v in agg.items()}

RAPTOR_BROWSERS = [
    "-chromium-cold",
    "-chromium-live",
    "-chromium",
    "-chrome-cold",
    "-chrome-live",
    "-chrome",
    "-fenix-cold-live",
    "-fenix-cold",
    "-fenix-live",
    "-fenix-power",
    "-fenoix",
    "-fenix-cold",
    "-fenix",
    "-fennec64-cold",
    "-fennec64-power",
    "-fennec64",
    "-fennec68-cold",
    "-fennec68-power",
    "-fennec68",
    "-fennec-cold",
    "-fennec-power",
    "-fennec",
    "-firefox-live-cumulative-power",
    "-firefox-live-utilization-power",
    "-firefox-live-watts-power",
    "-firefox-live-frequency-cpu-power",
    "-firefox-live-frequency-gpu-power",
    "-firefox-live",
    "-firefox-cold",
    "-firefox",
    "-geckoview-cpu",
    "-geckoview-cold",
    "-geckoview-live",
    "-geckoview-memory",
    "-geckoview-power",
    "-geckoview-%change-power",
    "-geckoview",
    "-refbrow-cold",
    "-refbrow-power",
    "-refbrow",
]

KNOWN_PERFHERDER_OPTIONS = ["pgo", "e10s", "stylo", "coverage"]

KNOWN_SUBTEST_PROPERTIES = {
    "alertChangeType": "alert.change_type",
    "alertThreshold": "alert.threshold",
    "base_replicates": "base_replicates",
    "lowerIsBetter": "lower_is_better",
    "name": "test",
    "ref_replicates": "ref_replicates",
    "replicates": None,
    "shouldAlert": "alert.enabled",
    "unit": "unit",
    "units": "unit",
    "value": None,
}

KNOWN_PERFHERDER_PROPERTIES = {
    "_id",
    "alertChangeType",
    "alertThreshold",
    "application",
    "etl",
    "extraOptions",
    "framework",
    "hgVersion",
    "is_empty",
    "lowerIsBetter",
    "name",
    "pulse",
    "results",
    "talos_counters",
    "test_build",
    "test_machine",
    "testrun",
    "serverUrl",
    "shouldAlert",
    "subtests",
    "summary",
    "tags",
    "type",
    "unit",
    "units",
    "value",
}
KNOWN_PERFHERDER_TESTS = [
    # BE SURE TO PUT THE LONGEST STRINGS FIRST
    "about_newtab_with_snippets",
    "about_preferences_basic",
    "ares6-sm",
    "ares6-v8",
    "ARES6",
    "a11yr",
    "avcodec section sizes",
    "avutil section sizes",
    "bcv",
    "Base Content Explicit",
    "Base Content Heap Unclassified",
    "Base Content JS",
    "Base Content Resident Unique Memory",
    "basic_compositor_video",
    "BenchCollections",
    "bloom_basic_ref",
    "bloom_basic_singleton",
    "bloom_basic",
    "build times",
    "bugbug_push_schedules_time",
    "bugbug_push_schedules_retries",
    "cart",
    "chromez",
    "chrome",
    "clone_errored",  # vcs
    "clone",  # vcs
    "compiler_metrics",
    "compiler warnings",
    "cpstartup",
    "damp",
    "debugger-metrics",
    "displaylist_mutate",
    "dromaeo_css",
    "dromaeo_dom",
    "dromaeojs",
    "Explicit Memory",
    "fetch_content",
    "flex",
    "GfxBench",
    "GfxQcmsPerf_Bgra",
    "GfxQcmsPerf_Rgba",
    "GfxQcmsPerf_Rgb",
    "g1",
    "g2",
    "g3",
    "g4-disabled",
    "g4",
    "g5",
    "glterrain",
    "glvideo",
    "h1",
    "h2",
    "Heap Unclassified",
    "ImageDecodersPerf_GIF_Rgb",
    "ImageDecodersPerf_JPG_YCbCr",
    "ImageDecodersPerf_JPG_Cmyk",
    "ImageDecodersPerf_JPG_Gray",
    "ImageDecodersPerf_PNG_RgbAlpha",
    "ImageDecodersPerf_PNG_Rgb",
    "ImageDecodersPerf_PNG_GrayAlpha",
    "ImageDecodersPerf_PNG_Gray",
    "ImageDecodersPerf_WebP_RgbLossless",
    "ImageDecodersPerf_WebP_RgbLossy",
    "ImageDecodersPerf_WebP_RgbAlphaLossless",
    "ImageDecodersPerf_WebP_RgbAlphaLossy",
    "Images",
    "inspector-metrics",
    "installer size",
    "JetStream",
    "jittest.jittest.overall",
    "JS",
    "kraken",
    "NSPR section sizes",
    "NSS section sizes",
    "mach_artifact_toolchain",
    "media_tests",
    "mochitest-browser-chrome-screenshots",
    "mochitest-browser-chrome",
    "motionmark_animometer",
    "motionmark_htmlsuite",
    "motionmark_webgl",
    "motionmark, transformed",
    "motionmark",
    "netmonitor-metrics",
    "octane-sm",
    "octane-v8",
    "os-baseline-power",
    "other_nol64",
    "other_l64",
    "other",
    "overall_clone_fullcheckout_rmstore",  # VCS
    "overall_clone_fullcheckout_rmwdir",
    "overall_clone_fullcheckout",  # VCS
    "overall_clone_rmwdir",
    "overall_clone",  # VCS
    "overall_nopull_fullcheckout",  # VCS
    "overall_nopull_populatedwdir",  # VCS
    "overall_nopull",  # VCS
    "overall_pull_fullcheckout",
    "overall_pull_emptywdir",
    "overall_pull_populatedwdir",
    "overall_pull_rmwdir",
    "overall_pull",  # VCS
    "overall",  # VCS
    "perf_reftest_singletons",
    "perf_reftest",  # THIS ONE HAS THE COMPARISION RESULTS
    "PermissionManagerTester",
    "PermissionManager",
    "pdfpaint",
    "pull_errored",  # VCS
    "pull",  # VCS
    "purge",  # VCS
    "Quantum_1",
    "quantum_pageload_amazon",
    "quantum_pageload_facebook",
    "quantum_pageload_google",
    "quantum_pageload_youtube",
    "rasterflood_gradient",
    "rasterflood_svg",
    "realworld-webextensions",
    "removed_missing_shared_store",
    "remove_locked_wdir",
    "remove_shared_store_active_lock",
    "Resident Memory",
    "sccache cache_write_errors",
    "sccache hit rate",
    "sccache requests_not_cacheable",
    "sessionrestore-many-windows",
    "sessionrestore_many_windows",
    "sessionrestore_no_auto_restore",
    "sessionrestore",
    "six-speed-sm",
    "six-speed-v8",
    "six-speed",
    "sparse_update_config",  # VCS
    "speedometer",
    "startup_about_home_paint_realworld_webextensions",
    "startup_about_home_paint_cached",
    "startup_about_home_paint",
    "Strings",
    "stylebench",
    "Stylo",
    "sunspider-sm",  # sm = spidermonkey
    "sunspider-v8",
    "sunspider",
    "svgr-disabled",
    "svgr",
    "tabpaint",
    "tabswitch",
    "tart_flex",
    "tart",
    "TestStandardURL",
    "total-after-gc",
    "TreeTraversal",
    "tcanvasmark",
    "tcheck2",
    "tp4m_nochrome",
    "tp4m",
    "tp5n",
    "tp5o_multiwindow_4_singlecp",
    "tp5o_scroll",
    "tp5o_webext",
    "tp5o",
    "tp6_amazon_heavy",
    "tp6_amazon",
    "tp6_facebook_heavy",
    "tp6_facebook",
    "tp6_google_heavy",
    "tp6_google",
    "tp6_youtube_heavy",
    "tp6_youtube",
    "tp6",
    "tp6-stylo-threads",
    "tpaint",
    "tps",
    "tresize",
    "trobocheck2",
    "ts_paint_webext",
    "ts_paint_heavy",
    "ts_paint_flex",
    "ts_paint",
    "tscrollx",
    "tsvgr_opacity",
    "tsvg_static",
    "tsvgx",
    "twinopen",
    "unity-webgl",
    "update_sparse",  # VCS
    "update",  # VCS
    "v8_7",
    "webconsole-metrics",
    "web-tooling-benchmark-sm",
    "web-tooling-benchmark-v8",
    "webgl",
    "webgl-gli",
    "xperf",
    "XUL section sizes",
]
