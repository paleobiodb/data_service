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
	cp -p -r /Volumes/pbdb_RAID/httpdocs/cgi-bin .
	
	# remove static stuff...
	rm cgi-bin/data/noaa.*
	rm cgi-bin/data/pmpd-*
	rm -R cgi-bin/data/masks2
	rm -R cgi-bin/data/nam

	tar -cf cgi-bin.tar cgi-bin
	gzip cgi-bin.tar
fi

# MAKE ARCHIVE OF HTML
if [ -x /Volumes/pbdb_RAID/httpdocs/html ]; then
	cp -p -r /Volumes/pbdb_RAID/httpdocs/html .
		
	# remove old user generated download files...
	rm -R html/public/data/*/*.txt
	rm -R html/public/data/*/*.csv
	rm -R html/public/data/*/*.tab
	rm -R html/public/maps/*.*
	rm -R html/nam
	rm html/paleodb/data/*.tab
 #       rm html/paleodb/data/*.txt
        rm html/paleodb/data/*.csv
	# rm -R html/paleodb/data

	# make archive
	tar -cf html.tar html
	gzip html.tar
fi

# MAKE ARCHIVE OF PALEOSOURCE
if [ -x /Volumes/pbdb_RAID/httpdocs/paleosource ]; then
	cp -p -r /Volumes/pbdb_RAID/httpdocs/paleosource .
	tar -cf paleosource.tar paleosource
	gzip paleosource.tar
fi
