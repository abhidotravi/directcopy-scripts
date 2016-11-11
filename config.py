#!/usr/bin/python

# Created by aravi

"""
This is a config file for stress profile
"""
src_volume_prefix = "dbvolume"
vol_start_index = 1
num_src_vols = 100

src_table_prefix = "srctable"
table_start_index = 1
num_src_tables = 100
num_cfs = 5
num_cols = 10
num_rows = 100000

# Replica specific
num_replica = 1
num_multimaster = 1
num_local = 1

local_replica_volume_name = "localvol"
remote_volume_name = "replvol"
remote_cluster_name = "zoom"
