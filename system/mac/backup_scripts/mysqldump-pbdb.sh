#!/bin/bash
MYSQL_BACKUP_DIR=/Volumes/pbdb_RAID/dailybackups/db
OF=mysql-backup-$(date +%Y%m%d)
LATEST=/Volumes/pbdb_RAID/dailybackups/pbdb_latest.gz

# mysqldump method (preferred)
mysqldump -u paleodbbackup -p'iIi4m2bd' --opt -B pbdb | gzip > $MYSQL_BACKUP_DIR/$OF.gz
cp $MYSQL_BACKUP_DIR/$OF.gz $LATEST
