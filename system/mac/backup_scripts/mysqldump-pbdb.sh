#!/bin/bash
MYSQL_BACKUP_DIR=/Volumes/pbdb_RAID/dailybackups
OF=mysql-backup-$(date +%Y%m%d)

# mysqldump method (preferred)
mysqldump -u paleodbbackup -p'iIi4m2bd' --opt -B pbdb | gzip > $MYSQL_BACKUP_DIR/$OF.gz
