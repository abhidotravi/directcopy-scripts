#!/usr/bin/python

#Created by aravi

import json
import os
import sys
import argparse
import re
import csv
import subprocess
from pprint import pprint

g_volume_prefix = "/dbvolume"
g_table_prefix = "/stable"
g_replvolume_prefix = "/replvol"
g_repltable_prefix = g_replvolume_prefix + "/rtable"
g_local_repltable_prefix ="/dbvolume/lrtable"
g_mm_repltable_prefix ="/replvol/mmrtable"
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



def create_table(start_idx, num_tables):
    for i in range(start_idx, start_idx + num_tables):
        print "Creating table " + g_volume_prefix + g_table_prefix + str(i).zfill(g_zfill_width)
        create_cmd = "maprcli table create -path " + g_volume_prefix + g_table_prefix + str(i).zfill(g_zfill_width)
        os.system(create_cmd)

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
    load_cmd = "/opt/mapr/server/tools/loadtest -mode put -table " + table_name + " -isjson false -numrows " + str(g_default_load_rows) + " -numfamilies " + str(g_num_families)
    os.system(load_cmd)

def create_table_load_data(start_idx, num_tables):
    create_table(start_idx, num_tables)

    for i in range(start_idx, start_idx + num_tables):
        load_test(g_volume_prefix + g_table_prefix + str(i))


def autosetup_replica(src_table_name, num_replica):
    print "Creating autosetup replica"
    # src_suffix = src_table_name[g_zfill_repl_width:]
    src_suffix = src_table_name.translate(None, '/')
    for i in range(0, num_replica):
        print "Creating replica " + g_replica_path + g_repltable_prefix + src_suffix + "_slave" + str(i)
        auto_setup_cmd = "maprcli table replica autosetup -path " + src_table_name + " -replica " + g_replica_path + g_repltable_prefix + src_suffix + "_slave" + str(i) + " -directcopy true"
        print auto_setup_cmd
        os.system(auto_setup_cmd)

def autosetup_intra_cluster_replica(src_table_name, num_replica):
    print "Intra cluster replica autosetup"
    # src_suffix = src_table_name[g_zfill_repl_width:]
    src_suffix = src_table_name.translate(None, '/')
    for i in range(0, num_replica):
        print "Creating Intra cluster replica " +  g_local_repltable_prefix + src_suffix + "_" + str(i)
        auto_setup_cmd = "maprcli table replica autosetup -path " + src_table_name + " -replica " + g_local_repltable_prefix + src_suffix + "_slave" + str(i) + " -directcopy true"
        print auto_setup_cmd
        os.system(auto_setup_cmd)

def multimaster_autosetup_replica(src_table_name, num_replica):
    print "Creating multimaster autosetup replica"
    # src_suffix = src_table_name[g_zfill_repl_width:]
    src_suffix = src_table_name.translate(None, '/')
    for i in range(0, num_replica):
        print "Creating Multimaster replica " + g_replica_path + g_mm_repltable_prefix + src_suffix + "_slave" + str(i)
        auto_setup_cmd = "maprcli table replica autosetup -path " + src_table_name + " -replica " + g_replica_path + g_mm_repltable_prefix + src_suffix + "_slave" + str(i) + " -directcopy true -multimaster true"
        print auto_setup_cmd
        os.system(auto_setup_cmd)
    # autosetup_replica(src_table_name, num_replica)
    # src_suffix = src_table_name[-1]
    # for i in range(0, ((num_replica/2) + 1)):
    #     auto_setup_cmd = "maprcli table replica autosetup -path " + g_replica_path + g_repltable_prefix + src_suffix + "_" + str(i) + " -replica " + src_table_name + " -directcopy true"
    #     os.system(auto_setup_cmd)


def try_seq_scenario(start_idx, num_tables):
    for i in range(0, num_tables):
        curr_table_name = g_volume_prefix + g_table_prefix + str(start_idx+i).zfill(g_zfill_width)
        create_single_table(curr_table_name)
        load_test(curr_table_name)
        autosetup_replica(curr_table_name, g_num_replica_tables)
        autosetup_intra_cluster_replica(curr_table_name, g_num_replica_tables)
        multimaster_autosetup_replica(curr_table_name, g_num_replica_tables)


def try_bulk_seq_scenario(start_idx, num_tables):

    list_of_tables = [g_volume_prefix + g_table_prefix + str(start_idx+i).zfill(g_zfill_width) for i in range(0, num_tables)]
    for curr_table_name in list_of_tables:
        create_single_table(curr_table_name)
        load_test(curr_table_name)

    for curr_table_name in list_of_tables:
        autosetup_replica(curr_table_name, g_num_replica_tables)
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
        autosetup_replica(table, g_num_replica_tables)
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
        autosetup_replica(table, g_num_replica_tables)
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
    for res in result_list:
        # load_test(res)
        autosetup_replica(res, 1)


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

    #There was no exception and result is not null
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






def usage():
    print "Usage: python dbreplication.py <option> <arguments>"
    print "Options:"
    print "\t-c <start_index> <num_tables>          - Create tables"
    print "\t-cl <start_index> <num_tables>         - Create table and load data"
    print "\t-d <start_index> <num_tables>          - Delete tables"
    print "\t-as <srctable> <num_replica>           - Autosetup replica"
    print "\t-al <srctable> <num_replica>           - Autosetup intracluster replica"
    print "\t-mm <srctable> <num_replica>           - Autosetup multimaster replica"
    print "\t-seq <start_index> <num_tables>    - seq scenario"
    print "\t-bulkset <start_index> <num_tables>    - Autosetup replica in bulk"
    print "\n\t-stress <start_index_vol> <num_vol> <start_index_table> <num_tables>       - Autosetup replica for stress"
    print "\t-stressbulk <start_index_vol> <num_vol> <start_index_table> <num_tables>   - Autosetup replica in bulk for stress"


if __name__ == "__main__":
    if len(sys.argv[1:]) < 1:
        usage()
        exit(-1)

    if sys.argv[1] == "-c":
        print "Create table"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        create_table(int(sys.argv[2]), int(sys.argv[3]))

    elif sys.argv[1] == "-cl":
        print "Create and load"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        create_table_load_data(int(sys.argv[2]), int(sys.argv[3]))

    elif sys.argv[1] == "-d":
        print "Delete table"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        delete_table(int(sys.argv[2]), int(sys.argv[3]))

    elif sys.argv[1] == "-as":
        print "Autosetup replica"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        autosetup_replica(sys.argv[2], int(sys.argv[3]))

    elif sys.argv[1] == "-al":
        print "Intracluster autosetup"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        autosetup_intra_cluster_replica(sys.argv[2], int(sys.argv[3]))

    elif sys.argv[1] == "-mm":
        print "Multimaster autosetup"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        multimaster_autosetup_replica(sys.argv[2], int(sys.argv[3]))

    elif sys.argv[1] == "-v":
        print "Create volume and table"
        if len(sys.argv[2:]) < 4:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        create_volumes_and_tables(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))

    elif sys.argv[1] == "-seq":
        print "seq scenario autosetup"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        try_seq_scenario(int(sys.argv[2]), int(sys.argv[3]))

    elif sys.argv[1] == "-bulkset":
        print "seq scenario bulk autosetup"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        try_bulk_seq_scenario(int(sys.argv[2]), int(sys.argv[3]))

    elif sys.argv[1] == "-stress":
        print "Create volume, table, autosetup replica in bulk"
        if len(sys.argv[2:]) < 4:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        stress_test_basic(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))

    elif sys.argv[1] == "-stressbulk":
        print "Create volume, table, autosetup replica in bulk"
        if len(sys.argv[2:]) < 4:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        stress_test_bulk(int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]))

    elif sys.argv[1] == "-av":
        print "Autosetup on tables in a volume"
        if len(sys.argv[2:]) < 1:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        autosetup_on_tables_in_volume(sys.argv[2])

    elif sys.argv[1] == "-percent":
        print "Get copytable percentage for a table"
        if len(sys.argv[2:]) < 2:
            print "Error: Please provide right arguments"
            usage()
            exit(-1)
        #get_copytable_percentage(sys.argv[2], sys.argv[3])

    else:
        usage()
