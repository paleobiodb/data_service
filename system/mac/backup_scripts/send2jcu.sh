#!/bin/sh

DATE=`/bin/date +%Y%m%d`
scp -p /Volumes/pbdb_RAID/dailybackups/mysql-backup-${DATE}.gz backup@sifr.jcu.edu.au:~/data/

scp -p /Users/backup/web/*.tar.gz backup@sifr.jcu.edu.au:~/web
