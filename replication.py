#!/usr/bin/python

#Created by aravi

import json
import os
import sys
import re
import subprocess
from pprint import pprint
import logging
import threading
import argparse
import utils

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s',)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    sub_parsers = parser.add_subparsers(help='command', dest='cmd_name')

    #create command
    create_parser = sub_parsers.add_parser('create')
    create_sub_parser = create_parser.add_subparsers(help='type', dest='obj_type')

    #create table command
    create_table_parser = create_sub_parser.add_parser('table', help='Create table / tables')
    create_table_parser.add_argument('tableprefix', help='Prefix for table path')
    create_table_parser.add_argument('-numtables', type=int, default=1, help='Number of tables')
    create_table_parser.add_argument('-startidx', type=int, default=1, help='Start index of table')

    #create volume command
    create_vol_parser = create_sub_parser.add_parser('volume', help='Create volume / volumes')
    create_vol_parser.add_argument('volumeprefix', help='Prefix for volume path')
    create_vol_parser.add_argument('-numvolumes', type=int, default=1, help='Number of volumes')
    create_vol_parser.add_argument('-startidx', type=int, default=1, help='Start index of table')

    #delete command
    delete_parser = sub_parsers.add_parser('delete')
    delete_sub_parser = delete_parser.add_subparsers(help='type', dest='obj_type')

    #delete table command
    delete_table_parser = delete_sub_parser.add_parser('table', help='Delete table / tables')
    delete_table_parser.add_argument('tableprefix', help='Prefix for table path')
    delete_table_parser.add_argument('-numtables', type=int, default=1, help='Number of tables')
    delete_table_parser.add_argument('-startidx', type=int, default=1, help='Start index of table')

    #delete volume command
    delete_vol_parser = delete_sub_parser.add_parser('volume', help='Delete volume / volumes')
    delete_vol_parser.add_argument('volumeprefix', help='Prefix for volume path')
    delete_vol_parser.add_argument('-numvolumes', type=int, default=1, help='Number of volumes')
    delete_vol_parser.add_argument('-startidx', type=int, default=1, help='Start index of table')

    #autopsetup command
    autosetup_parser = sub_parsers.add_parser('autosetup')
    autosetup_sub_parser = autosetup_parser.add_subparsers(help='type', dest='obj_type')

    #autosetup table command
    autosetup_table_parser = autosetup_sub_parser.add_parser('table', help='Create autosetup for a table')
    autosetup_table_parser.add_argument('srcpath', help='Source table path')
    autosetup_table_parser.add_argument('replpath', help='Path to parent directory of replica table')
    autosetup_table_parser.add_argument('-numreplica', type=int, default=1, help='Number of replicas')
    autosetup_table_parser.add_argument('-multimaster', type=bool, default=False, help="Setup Multimaster replica")

    #autosetup volume command
    autosetup_vol_parser = autosetup_sub_parser.add_parser('volume', help='Create autosetup for tables in a volume')
    autosetup_vol_parser.add_argument('volumepath', help='Volume path')
    autosetup_vol_parser.add_argument('replpath', help='Path to parent directory of replica table')
    autosetup_vol_parser.add_argument('-numreplica', type=int, default=1, help='Number of replicas')
    autosetup_vol_parser.add_argument('-multimaster', action='store_true', help="Setup Multimaster replica")

    args = parser.parse_args()
    print args

    if args.cmd_name == 'create':
        logging.debug('Create command')
        if args.obj_type == 'table':
            utils.create_table(table_path_prefix=args.tableprefix, start_idx=args.startidx, num_tables=args.numtables)
        elif args.obj_type == 'volume':
            utils.create_volume(volume_path_prefix=args.volumeprefix, start_idx=args.startidx, num_volumes=args.numvolumes)
    # elif args.cmd_name



