#!/bin/sh

DATE=`/bin/date +%Y%m%d`
scp "/Volumes/pbdb_RAID/dailybackups/db/mysql-backup-${DATE}.gz" backup@paleobackup.nceas.ucsb.edu:./
