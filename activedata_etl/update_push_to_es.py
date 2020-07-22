# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division, unicode_literals

import re

from boto import ec2 as boto_ec2

from jx_python import jx
from mo_collections import UniqueIndex
from mo_dots import unwrap, wrap
from mo_dots.objects import datawrap
from mo_fabric import Connection
from mo_files import TempFile, File
from mo_logs import Log, startup, constants
from mo_threads import Thread
from pyLibrary.aws import aws_retry


@aws_retry
def _get_managed_spot_requests(ec2_conn, name):
    output = wrap([
        datawrap(r)
        for r in ec2_conn.get_all_spot_instance_requests()
        if not r.tags.get("Name") or r.tags.get("Name").startswith(name)
    ])
    return output


@aws_retry
def _get_managed_instances(ec2_conn, name):
    requests = UniqueIndex(["instance_id"], data=_get_managed_spot_requests(ec2_conn, name).filter(lambda r: r.instance_id != None))
    reservations = ec2_conn.get_all_instances()

    output = []
    for res in reservations:
        for instance in res.instances:
            if instance.tags.get('Name', '').startswith(name) and instance._state.name == "running":
                instance.request = requests[instance.id]
                output.append(datawrap(instance))
    return wrap(output)


def _disable_oom_on_es(conn):
    with conn.warn_only():
        conn.sudo("supervisorctl start es")

    with conn.cd("/home/ec2-user"):
        conn.run("mkdir -p temp")
    with conn.cd("/home/ec2-user/temp"):
        processes = conn.sudo("ps -eo pid,command | grep java")
        candidates = [
            line
            for line in processes.stdout.split("\n")
            if "/usr/java/default/bin/java -Xms" in line and "org.elasticsearch.bootstrap.Elasticsearch" in line
        ]
        if not candidates:
            Log.error("Expecting to find some hint of Elasticsearch running")
        elif len(candidates) > 1:
            Log.error("Found more than one Elasticsearch running, not sure what to do")

        pid = candidates[0].strip().split(" ")[0].strip()
        conn.run("echo -16 > oom_adj")
        conn.sudo("sudo cp oom_adj /proc/" + pid + "/oom_adj")


def _start_indexer(config, instance, please_stop):
    try:
        Log.note("Start push_to_es at {{ip}}", ip=instance.ip_address)
        with Connection(kwargs=config, host=instance.ip_address) as conn:
            conn.sudo("supervisorctl start push_to_es:*")

    except Exception as e:
        Log.warning(
            "could not start {{instance_id}} ({{name}}) at {{ip}}",
            instance_id=instance.id,
            name=instance.tags["Name"],
            ip=instance.ip_address,
            cause=e
        )


def _stop_indexer(config, instance, please_stop):
    try:
        Log.note("Stop push_to_es at {{ip}}", ip=instance.ip_address)
        with Connection(kwargs=config, host=instance.ip_address) as conn:
            conn.sudo("supervisorctl stop push_to_es:*")

    except Exception as e:
        Log.warning(
            "could not stop {{instance_id}} ({{name}}) at {{ip}}",
            instance_id=instance.id,
            name=instance.tags["Name"],
            ip=instance.ip_address,
            cause=e
        )

def _upgrade_elasticsearch(config, instance, please_stop):
    # copy image of new es


    # copy new config file(s)
    # ENSURE CONFIG EXPECTS TWO MASTERS


    # bounce es
    try:
        with Connection(kwargs=config, host=instance.ip_address) as conn:
            # stop es instance
            conn.sudo("supervisorctl stop es")

            # backup config file
            conn.run("rm -fr ~/temp", warn=True)
            conn.run("mkdir ~/temp")
            with TempFile() as tempfile:
                conn.get("/usr/local/elasticsearch/config/elasticsearch.yml", tempfile)
                # ENSURE CONFIG EXPECTS TWO MASTERS
                tempfile.write(tempfile.read().replace("discovery.zen.minimum_main_nodes: 1", "discovery.zen.minimum_main_nodes: 2"))
                conn.put(tempfile, "/usr/local/elasticsearch/config/elasticsearch.yml")
            conn.run("cp /usr/local/elasticsearch/config/* ~/temp")

            # copy image of new es
            conn.put("resources/binaries/elasticsearch-6.5.4.tar.gz", "~/elasticsearch-6.5.4.tar.gz")
            conn.run("tar zxfv elasticsearch-6.5.4.tar.gz")
            conn.run("cp -R elasticsearch-6.5.4/* /usr/local/elasticsearch/")
            conn.run("rm -fr elasticsearch*")

            # COPY CONFIG FILES
            conn.run("cp  ~/temp/* /usr/local/elasticsearch/config")

            # START ES
            conn.sudo("supervisorctl start es")

    except Exception as e:
        Log.warning(
            "could not upgrade ES {{instance_id}} ({{name}}) at {{ip}}",
            instance_id=instance.id,
            name=instance.tags["Name"],
            ip=instance.ip_address,
            cause=e
        )



WHITESPACE = re.compile(r"\s+")


def _restart_indexxer(conn):
    conn.sudo("supervisorctl status")

    result = conn.sudo("supervisorctl restart push_to_es:*")
    if "unix:///tmp/supervisor.sock no such file" not in result:
        return

    # HUNT DOWN THE PROCESS AND TELL IT TO EXIT
    result = conn.run("ps -eo pid,%cpu,%mem,command | grep pypy")
    for line in result.stdout.split("\n"):
        if "/home/ec2-user/pypy/bin/pypy" in line:
            pid, cpu, mem, _ = WHITESPACE.split(line.strip(), 3)
            if float(cpu) < 10:  # ASSUME THIS IS NOT WORKING (WE SHOULD LOOK AT THE LOGS)
                conn.run("kill -9 " + pid)
            else:
                conn.run("kill -SIGINT " + pid)


def _update_indexxer(config, instance, please_stop):
    Log.note(
        "Update indexing {{instance_id}} ({{name}}) at {{ip}}",
        instance_id=instance.id,
        name=instance.tags["Name"],
        ip=instance.ip_address
    )
    try:
        with Connection(kwargs=config, host=instance.ip_address) as conn:
            # _update_ssh(conn)
            with conn.cd("/usr/local/elasticsearch"):
                conn.sudo("rm -f java*.hprof")

            _disable_oom_on_es(conn)
            with conn.cd("/home/ec2-user/ActiveData-ETL/"):
                result = conn.run("git pull origin push-to-es6")
                if "Already up-to-date." in result or "Already up to date." in result:
                    Log.note("No change required")
                else:
                    # RESTART ANYWAY, SO WE USE LATEST INDEX
                    conn.run("~/pypy/bin/pypy -m pip install -r requirements.txt")
                    with conn.warn_only():
                        _restart_indexxer(conn)
    except Exception as e:
        Log.warning(
            "could not update {{instance_id}} ({{name}}) at {{ip}}",
            instance_id=instance.id,
            name=instance.tags["Name"],
            ip=instance.ip_address,
            cause=e
        )


def _update_ssh(conn):
    public_key = File("d:/activedata.pub.ssh")
    with conn.cd("/home/ec2-user"):
        conn.put(public_key, ".ssh/authorized_keys")
        conn.run("chmod 600 .ssh/authorized_keys")


def _start_supervisor(conn):
    conn.put("~/code/SpotManager/examples/config/es_supervisor.conf", "/etc/supervisord.conf", use_sudo=True)

    # START DAEMON (OR THROW ERROR IF RUNNING ALREADY)
    with conn.warn_only():
        conn.sudo("supervisord -c /etc/supervisord.conf")

    conn.sudo("supervisorctl reread")
    conn.sudo("supervisorctl update")


def main():
    try:
        settings = startup.read_settings(defs=[
            {
                "name": ["--start"],
                "help": "start the push_to_es processes",
                "action": "store_true",
                "dest": "start",
                "default": False,
                "required": False
            },
            {
                "name": ["--stop"],
                "help": "stop the push_to_es processes",
                "action": "store_true",
                "dest": "stop",
                "default": False,
                "required": False
            },
            {
                "name": ["--update"],
                "help": "update the push_to_es processes, and bounce",
                "action": "store_true",
                "dest": "update",
                "default": False,
                "required": False
            }
        ])
        constants.set(settings.constants)
        Log.start(settings.debug)

        aws_args = dict(
            region_name=settings.aws.region,
            aws_access_key_id=unwrap(settings.aws.aws_access_key_id),
            aws_secret_access_key=unwrap(settings.aws.aws_secret_access_key)
        )
        ec2_conn = boto_ec2.connect_to_region(**aws_args)
        instances = _get_managed_instances(ec2_conn, settings.name)

        if settings.args.stop:
            method = _stop_indexer
        elif settings.args.start:
            method = _start_indexer
        elif settings.args.update:
            method = _update_indexxer
        else:
            raise Log.error("Expecting --start or --stop or --update")

        for g, ii in jx.chunk(instances, size=20):
            threads = [
                Thread.run(i.name, method, settings.fabric, i)
                for i in ii
            ]

            for t in threads:
                t.join()
    except Exception as e:
        Log.error("Problem with etl", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
