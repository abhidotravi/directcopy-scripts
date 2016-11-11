#!/usr/bin/python

# Created by aravi

import json
import os
import sys
import argparse
import re
import csv
import subprocess
from pprint import pprint
import logging
from threading import Thread, Lock

g_volume_prefix = "/dbvolume"
g_table_prefix = "/stable"
g_replvolume_prefix = "/replvol"
g_repltable_prefix = g_replvolume_prefix + "/rtable"
g_local_repltable_prefix = "/dbvolume/lrtable"
g_mm_repltable_prefix = "/replvol/mmrtable"
g_replica_path = "/mapr/zoom"

g_default_load_rows = 100000
g_num_families = 5
g_num_replica_tables = 1
g_zfill_width = 5
g_zfill_repl_width = -5

g_thread_count = 10
g_all_replica_fields = ['cluster', 'table', 'type', 'realTablePath', 'replicaState', 'paused',
                        'throttle', 'idx', 'networkencryption', 'synchronous', 'networkcompression',
                        'isUptodate', 'minPendingTS', 'maxPendingTS', 'bytesPending', 'putsPending',
                        'bucketsPending', 'uuid', 'copyTableCompletionPercentage']


def create_volume(volume_path_prefix, start_idx, num_volumes):
    """
    Creates volume(s) with specified path as prefix.
    :param volume_path_prefix: volume mount path (will be used as prefix for volume name)
    :param start_idx: start index appended to volume prefix
    :param num_volumes: number of volumes to be created
    :return: list of volume paths created
    """
    list_of_volumes = [volume_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in xrange(0, num_volumes)]
    logging.debug(list_of_volumes)
    for vol in list_of_volumes:
        create_vol_cmd = "maprcli volume create -name " + vol[1:] + " -path " + vol + " -replication 3 -topology /data"
        logging.info(create_vol_cmd)
        os.system(create_vol_cmd)
    return list_of_volumes


def delete_volume(volume_path_prefix, start_idx, num_volumes):
    """
    Deletes volume(s) with specified path as prefix.
    :param volume_path_prefix:
    :param start_idx:
    :param num_volumes:
    :return: list of volumes deleted
    """
    list_of_volumes = [volume_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in xrange(0, num_volumes)]
    logging.debug(list_of_volumes)
    for vol in list_of_volumes:
        delete_vol_cmd = "maprcli volume remove -name " + vol[1:] + " -force true"
        logging.info(delete_vol_cmd)
        os.system(delete_vol_cmd)
    return list_of_volumes


def create_table(table_path_prefix, start_idx=1, num_tables=1):
    """
    Creates table(s) with specified path as prefix.
    :param table_path_prefix: table path that serves as a prefix
    :param start_idx: start index of table (default = 1)
    :param num_tables: number of table to create (default = 1)
    :return: list of table names created
    """

    list_of_tables = [table_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in xrange(0, num_tables)]
    logging.debug(list_of_tables)
    for table_name in list_of_tables:
        # create_cmd = "maprcli table create -path " + g_volume_prefix + g_table_prefix + str(i).zfill(g_zfill_width)
        create_cmd = "maprcli table create -path " + table_name
        logging.info(create_cmd)
        os.system(create_cmd)
    return list_of_tables


def create_table_many(table_path_prefix_list, start_idx=1, num_tables=1):
    """
    Creates table(s) with specified paths as prefixes.
    :param table_path_prefix_list: list of table paths that serve as a prefixes
    :param start_idx: start index of table (default = 1)
    :param num_tables: number of table to create (default = 1)
    :return: list of table names created
    """
    map(lambda table: create_table(table, start_idx, num_tables), table_path_prefix_list)


def create_tables_multithread(table_path_prefix_list, start_idx=1, num_tables=1):
    """
    Creates table(s) with specified paths as prefixes.
    :param table_path_prefix_list: list of table paths that serve as a prefixes
    :param start_idx: start index of table (default = 1)
    :param num_tables: number of table to create (default = 1)
    :return: list of table names created
    """
    logging.debug("Creating tables from a list of prefixes")
    prefix_list_length = (len(table_path_prefix_list) + 1) / g_thread_count
    if prefix_list_length is 0:
        prefix_list_length = 1
    sublist_of_prefixes = [table_path_prefix_list[i:i + prefix_list_length] for i in
                           xrange(0, len(table_path_prefix_list), prefix_list_length)]
    logging.info(sublist_of_prefixes)

    threads = [Thread(target=create_table_many, args=(sublist_of_prefixes[i], start_idx, num_tables))
               for i in xrange(0, len(sublist_of_prefixes))]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logging.debug("Done")


def delete_table(table_path_prefix, start_idx=1, num_tables=1):
    """
    Deletes table(s) with specified path as prefix.
    :param table_path_prefix: table path that serves as a prefix
    :param start_idx: start index of table (default = 1)
    :param num_tables: number of table to create (default = 1)
    :return: list of table names deleted
    """

    list_of_tables = [table_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in xrange(0, num_tables)]
    logging.debug(list_of_tables)
    for table_name in list_of_tables:
        delete_cmd = "maprcli table delete -path " + table_name
        logging.info(delete_cmd)
        os.system(delete_cmd)
    return list_of_tables


def autosetup_replica_table(src_table, replica_parent, num_replica=1, is_multimaster=False):
    """
    Sets up replica for a given table. Can specify multimaster option.
    Replica table name is auto-generated.
    :param src_table: source table path
    :param replica_parent: path to the parent directory of replica table
    :param num_replica: number of replicas
    :param is_multimaster: is it a multimaster replica
    :return: list of replica tables
    """
    logging.debug("Creating autosetup replica")
    # Different table name for replica table and multimaster replica table
    rtable_prefix = "/rtable" if is_multimaster is False else "/mmrtable"
    # Remove leading forward slashes if any
    replica_parent = replica_parent[:-1] if replica_parent[-1:] == '/' else replica_parent
    # Generate replica table name
    src_suffix = src_table.translate(None, '/')

    list_of_replica = [replica_parent + rtable_prefix + src_suffix + "_slave" + str(i + 1) for i in
                       xrange(0, num_replica)]
    logging.debug(list_of_replica)

    for repl_table in list_of_replica:
        auto_setup_cmd = "maprcli table replica autosetup -path " + src_table + " -replica " + repl_table + " -directcopy true"
        if is_multimaster is True:
            auto_setup_cmd += " -multimaster true"
        logging.info(auto_setup_cmd)
        os.system(auto_setup_cmd)

    return list_of_replica


def autosetup_replica_volume(volume_path, replica_parent, num_replica=1, is_multimaster=False):
    """
    Autosetup replicas for all tables within a volume. Replica table names are auto-generated.
    :param volume_path: volume, whose tables should have replica autosetup
    :param replica_parent: path in which replica tables should be created
    :param num_replica: number of replicas
    :param is_multimaster: is it a multimaster replica
    :return: list of replica tables setup
    """
    logging.debug("Autosetup for tables in a volume")
    list_of_tables = get_tables_in_volume(volume_path)
    logging.info(list_of_tables)

    list_of_replica = map(lambda tab: autosetup_replica_table(tab, replica_parent, num_replica, is_multimaster),
                          list_of_tables)
    list_of_replica = [table for sub_list in list_of_replica for table in sub_list]
    logging.info(list_of_replica)
    return list_of_replica


def autosetup_many_replica_table(list_of_tables, replica_parent, num_replica=1, is_multimaster=False):
    """
    Sets up replica for tables in list. Can specify multimaster option.
    Replica table name is auto-generated.
    :param list_of_tables: list of source table path
    :param replica_parent: path to the parent directory of replica table
    :param num_replica: number of replicas
    :param is_multimaster: is it a multimaster replica
    :return: list of replica tables
    """
    logging.debug("Replica autosetup for all tables")
    map(lambda tab: autosetup_replica_table(tab, replica_parent, num_replica, is_multimaster),
        list_of_tables)


def autosetup_replica_table_multithread(volume_path, replica_parent, num_replica=1, is_multimaster=False):
    """
    Autosetup replicas for all tables within a volume. Faster execution - Multithreaded
    :param volume_path: volume, whose tables should have replica autosetup
    :param replica_parent: path in which replica tables should be created
    :param num_replica: number of replicas
    :param is_multimaster: is it a multimaster replica
    :return: None
    """
    logging.debug("Autosetup for tables in a volume")
    list_of_tables = get_tables_in_volume(volume_path)
    table_list_length = ((len(list_of_tables) + 1) / g_thread_count)
    if table_list_length is 0:
        table_list_length = 1
    sublist_of_tables = [list_of_tables[i:i + table_list_length] for i in
                         xrange(0, len(list_of_tables), table_list_length)]
    threads = [Thread(target=autosetup_many_replica_table,
                      args=(sublist_of_tables[i], replica_parent, num_replica, is_multimaster))
               for i in xrange(0, len(sublist_of_tables))]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logging.debug("Done")


def get_tables_in_volume(volume_path):
    """
    Get the table names of all tables within a volume
    :param volume_path: volume of interest
    :return: list of tables
    """
    logging.debug('Getting tables in a volume')
    result = None
    result_list = []
    cmd = "hadoop fs -ls " + volume_path + " | grep -v Found | awk \'{print $8}\'"
    try:
        result = subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError:
        logging.error(result)
        return result_list
    result_list = result.split()
    return result_list


def load_table(table_name, num_cfs=1, num_cols=3, num_rows=100000, is_json=False):
    """
    Uses load test to load data on to the table
    :param table_name:
    :param num_cfs: number of cf to put the data across
    :param num_cols: number of columns in each cf
    :param num_rows: total number of rows to insert
    :param is_json: puts data in to json table if specified
    :return: None
    """
    logging.debug("Loading data on to table")
    load_cmd = "/opt/mapr/server/tools/loadtest -mode put -table " + table_name \
               + " -numfamilies " + str(num_cfs) + " -numcols " + str(num_cols) \
               + " -numrows " + str(num_rows)
    if is_json is True:
        load_cmd += " -isjson true"
    logging.info(load_cmd)
    os.system(load_cmd)


def load_volume_tables(volume_path, num_cfs=1, num_cols=3, num_rows=100000, is_json=False):
    """
    Loads data on to all tables in the volume
    :param volume_path: name of the volume
    :param num_cfs: number of cf to put the data across
    :param num_cols: number of columns in each cf
    :param num_rows: total number of rows to insert
    :param is_json: puts data in to json table if specified
    :return: None
    """
    logging.debug("Loading data on to all tables in a volume")
    map(lambda tab: load_table(tab, num_cfs, num_cols, num_rows, is_json),
        get_tables_in_volume(volume_path))


def load_many_tables(list_of_tables, num_cfs=1, num_cols=3, num_rows=100000, is_json=False):
    """
    Loads data on to all tables provided in the list
    Faster because of multithreading
    :param list_of_tables: list of tables
    :param num_cfs: number of cf to put the data across
    :param num_cols: number of columns in each cf
    :param num_rows: total number of rows to insert
    :param is_json: puts data in to json table if specified
    :return: None
    """

    logging.debug("Loading data on to a list of tables")
    logging.debug(list_of_tables)
    map(lambda tab: load_table(tab, num_cfs, num_cols, num_rows, is_json), list_of_tables)


def load_volume_tables_multithread(volume_path, num_cfs=1, num_cols=3, num_rows=100000, is_json=False):
    """
    Loads data on to all tables in the volume.
    Faster because of multithreading
    :param volume_path: name of the volume
    :param num_cfs: number of cf to put the data across
    :param num_cols: number of columns in each cf
    :param num_rows: total number of rows to insert
    :param is_json: puts data in to json table if specified
    :return: None
    """
    logging.debug("Loading data on to all tables in a volume")
    list_of_tables = get_tables_in_volume(volume_path)
    table_list_length = ((len(list_of_tables) + 1) / g_thread_count)
    if table_list_length is 0:
        table_list_length = 1
    sublist_of_tables = [list_of_tables[i:i + table_list_length] for i in
                         xrange(0, len(list_of_tables), table_list_length)]
    threads = [Thread(target=load_many_tables,
                      args=(sublist_of_tables[i], num_cfs, num_cols, num_rows, is_json))
               for i in xrange(0, len(sublist_of_tables))]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logging.debug("Done")


def get_replica_status(table_name, fields):
    """
    Gets status of all replicas of a table
    User can provide a set of fields that need to be retrieved.
    By default all fields are displayed

    :param table_name: name of the table (path)
    :param fields: filter particular fields from replica status
    :return: None
    """

    fields_to_track = []
    fields_not_to_track = []
    if fields is not None:
        fields_to_track = fields.split(',')
        fields_not_to_track = [element for element in g_all_replica_fields if element not in fields]
        fields_to_track = [element2 for element2 in g_all_replica_fields if element2 not in fields_not_to_track]
    else:
        fields_to_track = g_all_replica_fields

    logging.info("Tracking following fields..")
    logging.info(fields_to_track)

    cmd_out = None
    replicalist_cmd = "maprcli table replica list -path " + table_name + " -json"
    logging.info(replicalist_cmd)

    try:
        cmd_out = subprocess.check_output(replicalist_cmd, shell=True)
    except subprocess.CalledProcessError:
        logging.error(cmd_out)
        return

    # There was no exception and result is not null
    json_out = json.loads(cmd_out)

    result = ""
    for data in json_out["data"]:
        result += "sourceTable: " + table_name
        for field in fields_to_track:
            result += ", " + field + ": " + str(data[field])
        if 'errors' in data:
            result += ", errors: " + str(data['errors'])
        result += "\n"

    # Not using the logging framework here
    print result


def get_replica_status_many(list_of_tables, fields):
    """
    Same as the above method. But tracks replica status for a list tables provided
    :param list_of_tables: list of tables for which replica status needs to be tracked
    :param fields: filter fields
    :return: None
    """
    logging.debug("Tracking replica for list of tables")
    logging.debug(list_of_tables)
    map(lambda table: get_replica_status(table, fields), list_of_tables)


def get_replica_status_multithread(volume_path, fields):
    """
    Fetches replica status of all the tables in a volume.
    To speed up the process, the method spawns multiple threads
    :param volume_path: volume path
    :param fields: filter fields
    :return: None
    """

    logging.debug("Tracking replica for tables in volume")
    list_of_tables = get_tables_in_volume(volume_path)
    table_list_length = ((len(list_of_tables) + 1) / g_thread_count)
    if table_list_length is 0:
        table_list_length = 1
    list_of_subtables = [list_of_tables[i:i + table_list_length] for i in
                         xrange(0, len(list_of_tables), table_list_length)]

    threads = [Thread(target=get_replica_status_many, args=(list_of_subtables[i], fields))
               for i in xrange(0, len(list_of_subtables))]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logging.debug("Done")
