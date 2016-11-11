#!/usr/bin/python

# Created by aravi

"""
This is a config file for stress profile
"""
# Source volume name prefix
src_volume_prefix = "dbvolume"
# Index will be appended to the end of volume
vol_start_index = 1
# Number of volumes
num_src_vols = 100
# Source table name prefix
src_table_prefix = "srctable"
# Index will be appended to the end of table name
table_start_index = 1
# Number of tables
num_src_tables = 100
# Number of column families in a table
num_cfs = 5
# Number of columns in a table
num_cols = 10
# Number of rows in a table
num_rows = 100000

# Number of cross-cluster replica for a table
num_replica = 1
# Number of multimaster replica for a table
num_multimaster = 1
# Number of intracluster replica for a table
num_local = 1

local_replica_volume_name = "localvol"
remote_volume_name = "replvol"
remote_cluster_name = "zoom"
