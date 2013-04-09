# This a wrapper for web_app.pl, to set the necessary environment variable
# so that DBD::mysql.pm can properly connect to the mysqld server.
#
export DYLD_LIBRARY_PATH=/usr/local/mysql/lib
/opt/local/bin/starman --listen :3000 --workers=2 bin/web_app.pl
