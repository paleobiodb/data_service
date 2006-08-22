echo "Changing directory to filtered";
if cd /Volumes/pbdb_RAID/httpdocs/html/admin/logs/filtered/; then
    cat /var/log/httpd/access_log | egrep 'listTaxa|listCollections' | cut -f1 -d' ' | sort | uniq > BOT_IPS
    cp webalizer_base.conf webalizer.conf.new
    cat BOT_IPS | awk '{print "IgnoreSite ",$1}' >> webalizer.conf.new
    cp webalizer.conf.new webalizer.conf
else 
    exit 1
fi
