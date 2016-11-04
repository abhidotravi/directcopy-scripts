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


def create_volumes_and_tables(start_idx_vol, num_volumes, start_idx_table, num_tables):
    list_of_volumes = [g_volume_prefix + str(start_idx_vol + i).zfill(g_zfill_width) for i in range(0, num_volumes)]
    print list_of_volumes

    for vol in list_of_volumes:
        print "Creating volume " + vol
        create_vol_cmd = "maprcli volume create -name " + vol[1:] + " -path " + vol + " -replication 3 -topology /data"
        print create_vol_cmd
        os.system(create_vol_cmd)

    list_of_table_names = [g_table_prefix + str(start_idx_table + i).zfill(g_zfill_width) for i in range(0, num_tables)]
    print list_of_table_names

    list_of_tables = [vol + tab for vol in list_of_volumes for tab in list_of_table_names]

    for table in list_of_tables:
        create_table_cmd = "maprcli table create -path " + table
        print create_table_cmd
        os.system(create_table_cmd)

    return list_of_tables

def create_single_table(table_name):
    print "Creating single table " + table_name
    create_cmd = "maprcli table create -path " + table_name
    os.system(create_cmd)


def delete_table(start_idx, num_tables):
    for i in range(start_idx, start_idx + num_tables):
        print "Delete table " + g_volume_prefix + g_table_prefix + str(i)
        create_cmd = "maprcli table delete -path " + g_volume_prefix + g_table_prefix + str(i)
        os.system(create_cmd)


def load_test(table_name):
    print "Loading data on to table"
    load_cmd = "/opt/mapr/server/tools/loadtest -mode put -table " + table_name + " -isjson false -numrows " + str(
        g_default_load_rows) + " -numfamilies " + str(g_num_families)
    os.system(load_cmd)


def create_table_load_data(start_idx, num_tables):
    create_table(start_idx, num_tables)

    for i in range(start_idx, start_idx + num_tables):
        load_test(g_volume_prefix + g_table_prefix + str(i))

def create_volume(volume_path_prefix, start_idx, num_volumes):
    """
    Creates volume(s) with specified path as prefix.
    :param volume_path_prefix: volume mount path (will be used as prefix for volume name)
    :param start_idx: start index appended to volume prefix
    :param num_volumes: number of volumes to be created
    :return: list of volume paths created
    """
    list_of_volumes = [volume_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in range(0, num_volumes)]
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
    :return:
    """
    list_of_volumes = [volume_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in range(0, num_volumes)]
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

    list_of_tables = [table_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in range(0, num_tables)]
    logging.debug(list_of_tables)
    for table_name in list_of_tables:
        #create_cmd = "maprcli table create -path " + g_volume_prefix + g_table_prefix + str(i).zfill(g_zfill_width)
        create_cmd = "maprcli table create -path " + table_name
        logging.info(create_cmd)
        os.system(create_cmd)
    return list_of_tables

def delete_table(table_path_prefix, start_idx=1, num_tables=1):
    """
    Deletes table(s) with specified path as prefix.
    :param table_path_prefix: table path that serves as a prefix
    :param start_idx: start index of table (default = 1)
    :param num_tables: number of table to create (default = 1)
    :return: list of table names deleted
    """

    list_of_tables = [table_path_prefix + str(start_idx + i).zfill(g_zfill_width) for i in range(0, num_tables)]
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
    #Different table name for replica table and multimaster replica table
    rtable_prefix = "/rtable" if is_multimaster is False else "/mmrtable"
    #Remove leading forward slashes if any
    replica_parent = replica_parent[:-1] if replica_parent[-1:] == '/' else replica_parent
    #Generate replica table name
    src_suffix = src_table.translate(None, '/')

    list_of_replica = [replica_parent + rtable_prefix + src_suffix + "_slave" + str(i+1) for i in range(0, num_replica)]
    logging.debug(list_of_replica)

    for repl_table in list_of_replica:
        auto_setup_cmd = "maprcli table replica autosetup -path " + src_table + " -replica " + repl_table+ " -directcopy true"
        if is_multimaster is True:
            auto_setup_cmd += " -multimaster true"
        logging.info(auto_setup_cmd)
        #os.system(auto_setup_cmd)

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

def autosetup_intra_cluster_replica(src_table_name, num_replica):
    print "Intra cluster replica autosetup"
    # src_suffix = src_table_name[g_zfill_repl_width:]
    src_suffix = src_table_name.translate(None, '/')
    for i in range(0, num_replica):
        print "Creating Intra cluster replica " + g_local_repltable_prefix + src_suffix + "_" + str(i)
        auto_setup_cmd = "maprcli table replica autosetup -path " + src_table_name + " -replica " + g_local_repltable_prefix + src_suffix + "_slave" + str(
            i) + " -directcopy true"
        print auto_setup_cmd
        os.system(auto_setup_cmd)


def multimaster_autosetup_replica(src_table_name, num_replica):
    print "Creating multimaster autosetup replica"
    # src_suffix = src_table_name[g_zfill_repl_width:]
    src_suffix = src_table_name.translate(None, '/')
    for i in range(0, num_replica):
        print "Creating Multimaster replica " + g_replica_path + g_mm_repltable_prefix + src_suffix + "_slave" + str(i)
        auto_setup_cmd = "maprcli table replica autosetup -path " + src_table_name + " -replica " + g_replica_path + g_mm_repltable_prefix + src_suffix + "_slave" + str(
            i) + " -directcopy true -multimaster true"
        print auto_setup_cmd
        os.system(auto_setup_cmd)
        # autosetup_replica(src_table_name, num_replica)
        # src_suffix = src_table_name[-1]
        # for i in range(0, ((num_replica/2) + 1)):
        #     auto_setup_cmd = "maprcli table replica autosetup -path " + g_replica_path + g_repltable_prefix + src_suffix + "_" + str(i) + " -replica " + src_table_name + " -directcopy true"
        #     os.system(auto_setup_cmd)


def try_seq_scenario(start_idx, num_tables):
    for i in range(0, num_tables):
        curr_table_name = g_volume_prefix + g_table_prefix + str(start_idx + i).zfill(g_zfill_width)
        create_single_table(curr_table_name)
        load_test(curr_table_name)
        #autosetup_replica(curr_table_name, g_num_replica_tables)
        autosetup_intra_cluster_replica(curr_table_name, g_num_replica_tables)
        multimaster_autosetup_replica(curr_table_name, g_num_replica_tables)


def try_bulk_seq_scenario(start_idx, num_tables):
    list_of_tables = [g_volume_prefix + g_table_prefix + str(start_idx + i).zfill(g_zfill_width) for i in
                      range(0, num_tables)]
    for curr_table_name in list_of_tables:
        create_single_table(curr_table_name)
        load_test(curr_table_name)

    for curr_table_name in list_of_tables:
        #autosetup_replica(curr_table_name, g_num_replica_tables)
        autosetup_intra_cluster_replica(curr_table_name, g_num_replica_tables)
        multimaster_autosetup_replica(curr_table_name, g_num_replica_tables)


def stress_test_bulk(start_idx_vol, num_volumes, start_idx_table, num_tables):
    """
    Used for stress / longevity tests. Creates multiple volumes with multiple tables.
    Replica autosetup for all tables that are created and loaded with data, is done in bulk
    """
    list_of_tables = create_volumes_and_tables(start_idx_vol, num_volumes, start_idx_table, num_tables)
    for table in list_of_tables:
        load_test(table_name=table)

    for table in list_of_tables:
        #autosetup_replica(table, g_num_replica_tables)
        autosetup_intra_cluster_replica(table, g_num_replica_tables)
        multimaster_autosetup_replica(table, g_num_replica_tables)


def stress_test_basic(start_idx_vol, num_volumes, start_idx_table, num_tables):
    """
    Used for stress / longevity tests. Creates multiple volumes with multiple tables.
    Load data and autosetup replica on each table, one by one
    """
    list_of_tables = create_volumes_and_tables(start_idx_vol, num_volumes, start_idx_table, num_tables)
    for table in list_of_tables:
        load_test(table_name=table)
        #autosetup_replica(table, g_num_replica_tables)
        autosetup_intra_cluster_replica(table, g_num_replica_tables)
        multimaster_autosetup_replica(table, g_num_replica_tables)


def autosetup_on_tables_in_volume(volumename):
    """
    Do autosetup for all tables in the volume
    :param volumename: Path of the volume
    :return:
    """
    cmd = "hadoop fs -ls " + volumename + " | grep stable | awk \'{print $8}\'"
    result = subprocess.check_output(cmd, shell=True)
    result_list = result.split()
    # for res in result_list:
        # load_test(res)
        #autosetup_replica(res, 1)


'''Bunch of Utility Methods'''


def get_replica_stats(srctable, stats):
    """
    Appends the copytable percentage for replicas of a srctable
    :param srctable: Source Table
    :param out_file_path: Path of the output file
    :return:
    """

    cmd_out = None
    replicalist_cmd = "maprcli table replica list -path " + srctable + " -json"
    print replicalist_cmd

    try:
        cmd_out = subprocess.check_output(replicalist_cmd, shell=True)
    except subprocess.CalledProcessError:
        print "ERROR:", cmd_out
        return

    # There was no exception and result is not null
    json_out = json.loads(cmd_out)

    result = "src: " + srctable

    for data in json_out["data"]:
        result += ", replica: " + data["table"]
        result += ", idx: " + data["idx"]
        for stat in stats:
            result += ", " + stat + ": " + data[stat]

        result += "\n"
        # data = result_json["data"]
        # for i in xrange(0, len(json_data["data"]))
        # pprint(json_data)
        # copytable_percent = json_data["copyTableCompletionPercentage"]