#!/bin/sh

# SET ARCHIVE FILE EXTENSION...
gzext=".gz";


# check privs...

# SET REPOSITORY
cd /Users/backup/web

# ROTATE EXISTING ARCHIVES
for i in cgi-bin.tar html.tar paleosource.tar; do
    if [ -f "${i}${gzext}" ]; then
	if [ -f "${i}.3${gzext}" ]; then mv -f "${i}.3${gzext}" "${i}.4${gzext}"; fi
	if [ -f "${i}.2${gzext}" ]; then mv -f "${i}.2${gzext}" "${i}.3${gzext}"; fi
	if [ -f "${i}.1${gzext}" ]; then mv -f "${i}.1${gzext}" "${i}.2${gzext}"; fi
	if [ -f "${i}.0${gzext}" ]; then mv -f "${i}.0${gzext}" "${i}.1${gzext}"; fi
	if [ -f "${i}${gzext}" ]; then mv -f "${i}${gzext}" "${i}.0${gzext}"; fi
    fi
done

# MAKE ARCHIVE OF CGI-BIN
if [ -x /Volumes/pbdb_RAID/httpdocs/cgi-bin ]; then
	cp -P -p -R /Volumes/pbdb_RAID/httpdocs/cgi-bin .
	
	# remove static stuff...
	rm cgi-bin/data/noaa.*
	rm cgi-bin/data/pmpd-*
    rm cgi-bin/data/platepolygons/*
	rm -R cgi-bin/data/masks2
	rm -R cgi-bin/data/nam

	# remove cgi-bin stuff that isn't explicity allowed. 
	find -E .  -regex '\./cgi-bin/.*'  -not -regex '\./cgi-bin/(data/.*|guest_templates.*|templates.*|bridge.pl|.*\.cgi|[A-Z][A-Za-z0-9]*\.pm)' -delete
	find . -regex '.*/\.#.*' -delete

	tar -cf cgi-bin.tar cgi-bin
	gzip cgi-bin.tar
fi

# MAKE ARCHIVE OF HTML
if [ -x /Volumes/pbdb_RAID/httpdocs/html ]; then
	cp -P -p -R /Volumes/pbdb_RAID/httpdocs/html .
		
	# remove old user generated download files...
	rm -R html/public/data/*/*.txt
	rm -R html/public/data/*/*.csv
	rm -R html/public/data/*/*.tab
	rm -R html/public/data/*/*.conjunct
	rm -R html/public/maps/*.ai
	rm -R html/public/maps/*.jpg
	rm -R html/public/maps/*.png
	rm -R html/public/maps/*.pict
	rm -R html/public/maps/*.html
	rm -R html/public/data/*.csv
	rm -R html/public/data/*.refs
	rm -R html/public/confidence/*.jpg
	rm -R html/public/confidence/*.ai
	rm -R html/public/confidence/*.png
	rm -R html/admin/logs/*/*.png
	rm -R html/admin/logs/*/*.html
	rm -R html/admin/logs/*/dns_cache.db
	rm -R html/nam
	rm html/paleodb/data/*.tab
   	rm html/paleodb/data/*.csv
  	rm html/paleodb/data/*.conjunct
 #      rm html/paleodb/data/*.txt
	rm -R html/paleodb/data

	# make archive
	tar -cf html.tar html
	gzip html.tar
fi

# MAKE ARCHIVE OF PALEOSOURCE
if [ -x /Volumes/pbdb_RAID/httpdocs/paleosource ]; then
	cp -P -p -R /Volumes/pbdb_RAID/httpdocs/paleosource .
	tar -cf paleosource.tar paleosource
	gzip paleosource.tar
fi
