
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

from jx_sqlite.sqlite import Sqlite, quote_value, quote_list, sql_insert
from mo_dots import wrap, coalesce
from mo_json import json2value, value2json
from mo_kwargs import override
from mo_logs import Log
from mo_threads import Till
from mo_times import Timer, Date
from pyLibrary import aws
from mo_http import http

DEBUG = True
SLEEP_ON_ERROR = 30
MAX_BAD_REQUESTS = 3


class TuidClient(object):

    @override
    def __init__(self, endpoint, push_queue=None, timeout=30, db=None, kwargs=None):
        self.enabled = True
        self.num_bad_requests = 0
        self.endpoint = endpoint
        self.timeout = timeout
        self.push_queue = aws.Queue(push_queue) if push_queue else None
        self.config = kwargs

        self.db = Sqlite(filename=coalesce(db.filename, "tuid_client.sqlite"), upgrade=False, kwargs=db)

        if not self.db.query("SELECT name FROM sqlite_main WHERE type='table';").data:
            with self.db.transaction() as transaction:
                self._setup(transaction)

    def _setup(self, transaction):
        transaction.execute("""
        CREATE TABLE tuid (
            revision CHAR(12),
            file TEXT,
            tuids TEXT,
            PRIMARY KEY(revision, file)
        )
        """)

    def get_tuid(self, branch, revision, file):
        """
        :param branch: BRANCH TO FIND THE REVISION/FILE
        :param revision: THE REVISION NUNMBER
        :param file: THE FULL PATH TO A SINGLE FILE
        :return: A LIST OF TUIDS
        """
        service_response = wrap(self.get_tuids(branch, revision, [file]))
        for f, t in service_response.items():
            return t

    def get_tuids(self, branch, revision, files):
        """
        GET TUIDS FROM ENDPOINT, AND STORE IN DB
        :param branch: BRANCH TO FIND THE REVISION/FILE
        :param revision: THE REVISION NUNMBER
        :param files: THE FULL PATHS TO THE FILES
        :return: MAP FROM FILENAME TO TUID LIST
        """

        # SCRUB INPUTS
        revision = revision[:12]
        files = [file.lstrip('/') for file in files]

        with Timer(
            "ask tuid service for {{num}} files at {{revision|left(12)}}",
            {"num": len(files), "revision": revision},
            silent=not DEBUG or not self.enabled
        ):
            response = self.db.query(
                "SELECT file, tuids FROM tuid WHERE revision=" + quote_value(revision) +
                " AND file IN " + quote_list(files)
            )
            found = {file: json2value(tuids) for file, tuids in response.data}

            try:
                remaining = set(files) - set(found.keys())
                new_response = None
                if remaining:
                    request = wrap({
                        "from": "files",
                        "where": {"and": [
                            {"eq": {"revision": revision}},
                            {"in": {"path": remaining}},
                            {"eq": {"branch": branch}}
                        ]},
                        "branch": branch,
                        "meta": {
                            "format": "list",
                            "request_time": Date.now()
                        }
                    })
                    if self.push_queue is not None:
                        if DEBUG:
                            Log.note("record tuid request to SQS: {{timestamp}}", timestamp=request.meta.request_time)
                        self.push_queue.add(request)
                    else:
                        if DEBUG:
                            Log.note("no recorded tuid request")

                    if not self.enabled:
                        return found

                    new_response = http.post_json(
                        self.endpoint,
                        json=request,
                        timeout=self.timeout
                    )

                    if new_response.data and any(r.tuids for r in new_response.data):
                        try:
                            with self.db.transaction() as transaction:
                                command = sql_insert("tuid", [
                                    {"revision": revision, "file": r.path, "tuids": value2json(r.tuids)}
                                    for r in new_response.data
                                    if r.tuids != None
                                ])
                                transaction.execute(command)
                        except Exception as e:
                            Log.error("can not insert {{data|json}}", data=new_response.data, cause=e)
                self.num_bad_requests = 0

                found.update({r.path: r.tuids for r in new_response.data} if new_response else {})
                return found

            except Exception as e:
                self.num_bad_requests += 1
                if self.enabled:
                    if "502 Bad Gateway" in e:
                        self.enabled = False
                        Log.alert("TUID service has problems (502 Bad Gateway)", cause=e)
                    elif self.num_bad_requests >= MAX_BAD_REQUESTS:
                        self.enabled = False
                        Log.alert("TUID service has problems (given up trying to use it)", cause=e)
                    else:
                        Log.alert("TUID service has problems.", cause=e)
                        Till(seconds=SLEEP_ON_ERROR).wait()
                return found
