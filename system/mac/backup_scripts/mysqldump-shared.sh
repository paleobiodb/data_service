#!/bin/bash
MYSQL_BACKUP_DIR=/Volumes/pbdb_RAID/dailybackups
OF=mysql-shared-$(date +%Y%m%d)

# mysqldump method (preferred)
mysqldump -u paleodbbackup -p'iIi4m2bd' --opt -B shared | gzip > $MYSQL_BACKUP_DIR/$OF.gz
