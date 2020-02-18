# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from jx_base.expressions import BooleanOp as BooleanOp_, FALSE, TRUE, is_literal
from jx_sqlite.expressions._utils import SQLang, check


class BooleanOp(BooleanOp_):
    @check
    def to_sql(self, schema, not_null=False, boolean=False):
        term = SQLang[self.term].partial_eval()
        if term.type == "boolean":
            sql = term.to_sql(schema)
            return sql
        elif is_literal(term) and term.value in ("T", "F"):
            if term.value == "T":
                return TRUE.to_sql(schema)
            else:
                return FALSE.to_sql(schema)
        else:
            sql = term.exists().partial_eval().to_sql(schema)
            return sql
