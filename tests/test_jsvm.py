# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: ActiveData Maintainers (active-data-maintainers@mozilla.com)
#
from __future__ import division
from __future__ import unicode_literals

import unittest

from activedata_etl.transforms.jsvm_to_es import process_jsvm_artifact
from mo_dots import Null, Data
from mo_times import Date
from tests import Destination


class TestJsdov(unittest.TestCase):

    def test_one_url(self):
        key = Null
        url = "https://firefox-ci-tc.services.mozilla.com/api/queue/v1/task/HOYgr2crRaKd6VTIw9ezOw/artifacts/public/test_info/code-coverage-jsvm.zip"
        destination = Destination("results/jsvm/lcov_parsing_result.json.gz")

        process_jsvm_artifact(
            source_key=key,
            resources=Data(),
            destination=destination,
            artifact=Data(url=url),
            task_cluster_record=Data(repo={"push": {"date": Date.now()}}),
            artifact_etl=Null,
            please_stop=Null
        )
