# 
# Paleobiology Database - Main API

FROM perl:5.26-threaded AS paleobiodb_api_loaded

RUN apt-get update && \
    apt-get -y install mariadb-client && \
    cpanm DBI && \
    cpanm DBD::mysql && \
    cpanm Dancer && \
    cpanm Moo && \
    cpanm namespace::clean && \
    cpanm YAML && \
    cpanm Template && \
    cpanm Dancer::Template::Tiny && \
    cpanm Dancer::Plugin::Database && \
    cpanm Dancer::Plugin::StreamData && \
    cpanm Starman
    
RUN cpanm HTTP::Validate && \
    cpanm Web::DataService

RUN cpanm --force Term::ReadLine::Gnu

FROM paleobiodb_api_loaded

COPY pbdb-new /var/paleobiodb/pbdb-new/

VOLUME /var/paleobiodb/pbdb-new/logs

EXPOSE 3000 3999

WORKDIR /var/paleobiodb/pbdb-new/

CMD perl bin/data_service.pl

LABEL maintainer="mmcclenn@geology.wisc.edu"
LABEL version="1.0"
LABEL description="Paleobiology Database data service"

LABEL buildcheck="bin/web_app.pl GET /data1.0/"





