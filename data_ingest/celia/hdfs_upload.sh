#!/bin/bash
# AO3 data ingest for Celia Liang (cl7093_nyu_edu)
#
# AO3 publishes selective data dumps periodically at:
#   https://archiveofourown.org/admin_posts/18804
#
# I used the Feb 2021 release, which yields:
#   works-20210226.csv  (~960 MB)
#   tags-20210226.csv
#
# After manually downloading and unpacking the tarball on a local machine,
# the CSVs are scp'd to the Dataproc master node and pushed to HDFS with:

set -euo pipefail

HDFS_INPUT_DIR="/user/cl7093_nyu_edu/course_project/input"

hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
hdfs dfs -put -f works-20210226.csv "${HDFS_INPUT_DIR}/"
hdfs dfs -put -f  tags-20210226.csv "${HDFS_INPUT_DIR}/"

hdfs dfs -ls "${HDFS_INPUT_DIR}"
