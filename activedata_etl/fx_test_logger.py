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
from mo_threads.threads import MAIN_THREAD

from mo_dots import wrap
from mo_logs import Log, startup, constants
from mo_math import Math
from mo_times import Date, DAY
from pyLibrary import aws
from pyLibrary.aws import s3
from jx_python import jx
from pyLibrary.sql.sqlite import Sqlite

ZERO_DAY = Date("1 jan 2015")


def make_db(db):
    result = db.query("SELECT name FROM sqlite_main WHERE type='table' AND name=" + db.quote_value("content"))

    if result.data:
        return

    db.execute("CREATE TABLE content (filename text, timestamp real, key1 int, key2 int)")


def loop(settings):
    source = s3.PublicBucket(kwargs=settings.source)
    destination = s3.Bucket(settings.destination)
    notify = aws.Queue(settings.notify)

    db = Sqlite(
        filename="./results/" + "s3." + settings.source.name + ".sqlite",
        upgrade=False
    )
    make_db(db)

    # FILL SQLITE
    dirty = []
    for g, cs in jx.chunk(source.list(), size=100):
        Log.note("scanning {{num}} files in {{source}}", num=100, source=source.url)
        result = db.query("SELECT filename, timestamp FROM content WHERE filename in (" + ",".join(map(db.quote_value, cs.key)) + ")")
        existing = [d[0] for d in result.data]
        if len(cs) - len(existing):
            db.execute(
                "INSERT INTO content(filename, timestamp) VALUES " +
                ",".join("(" + db.quote_value(c.key) + "," + db.quote_value(Date(c.lastmodified)) + ")" for c in cs if c.key not in existing)
            )
        for c in cs:
            if c.key in existing and (c.key, Date(c.lastmodified).unix) not in result.data:
                dirty.append(c.key)
                db.execute("UPDATE content SET timestamp=" + db.quote_value(c.lastmodified) + " WHERE filename=" + db.quote_value(c.key))

    latest_timestamp = Date(db.query("SELECT MAX(timestamp) FROM content").data[0][0])
    Log.note("Last known file is {{timestamp}}", timestamp=latest_timestamp)

    result = db.query(
        "SELECT filename, key1, key2, timestamp" +
        " FROM content" +
        " WHERE key1 IS NULL " + ("" if not dirty else ("or filename in (" + ",".join(db.quote_value(d) for d in dirty) + ")")) +
        " ORDER BY timestamp, filename"
    )
    data = wrap([{k: d[i] for i, k in enumerate(result.header)} for d in result.data])

    Log.note("{{num}} new files found", num=len(data))
    maxi = {}
    for d in data:
        Log.note("Update record {{filename}}", filename=d.filename)
        if d.key1 == None:
            day = mo_math.floor((Date(d.timestamp) - ZERO_DAY) / DAY)
            if maxi.get(day) == None:
                max_key = db.query("SELECT max(key2) FROM content WHERE key1=" + db.quote_value(day)).data[0][0]
                if max_key == None:
                    maxi[day] = 0
                else:
                    maxi[day] = max_key + 1
            d.key1 = day
            d.key2 = maxi[day]
            maxi[day] += 1

        full_key = text(d.key1) + "." + text(d.key2)
        destination.write_lines(key=full_key, lines=source.read_lines(d.filename))
        notify.add({
            "bucket": destination.bucket.name,
            "key": full_key,
            "timestamp": Date.now(),
            "date/time": Date.now().format()
        })

        db.execute(
            "UPDATE content SET" +
            " key1=" + db.quote_value(d.key1) + "," +
            " key2=" + db.quote_value(d.key2) + "," +
            " timestamp=" + db.quote_value(d.timestamp) +
            " WHERE filename=" + db.quote_value(d.filename)
        )


def main():
    try:
        settings = startup.read_settings()
        with startup.SingleInstance(flavor_id=settings.args.filename):
            constants.set(settings.constants)
            Log.start(settings.debug)
            loop(settings)
    except Exception as e:
        Log.warning("Problem with logging", cause=e)
    finally:
        Log.stop()
        MAIN_THREAD.stop()


if __name__ == "__main__":
    main()

