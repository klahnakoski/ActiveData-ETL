# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (klahnakoski@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

import unittest

from activedata_etl.transforms import cov_to_es
from activedata_etl.transforms.jscov_to_es import process_jscov_artifact
from mo_dots import Null, Data
from pyLibrary.aws.s3 import PublicBucket
from test_gcov import Destination


class TestJsdov(unittest.TestCase):
    def test_one_url(self):
        key=Null
        url="http://queue.taskcluster.net/v1/task/GKlTCjJ1QMSgoTQbqAhrbg/artifacts/public/test_info//jsdcov_artifacts.zip"
        destination = Destination("results/jsdcov/lcov_parsing_result.json.gz")

        process_jscov_artifact(
            source_key=key,
            resources=Null,
            destination=destination,
            artifact=Data(url=url),
            task_cluster_record=Null,
            artifact_etl=Null,
            please_stop=Null
        )

    # def test_etl_block(self):
    #     source = Data(read_lines=lambda: PublicBucket("https://s3-us-west-2.amazonaws.com/active-data-task-cluster-normalized").read_lines("tc.1051816:105180763.json.gz"))
    #     destination = Destination("results/ccov/lcov_output.gz")
    #
    #     cov_to_es.process(
    #         "tc.1051816",
    #         source=source,
    #         destination=destination,
    #         resources=Null,
    #     )
