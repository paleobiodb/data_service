# 
# Paleobiology Database - Main API image
# 
# The image 'paleomacro_pbapi_preload' is built from the file 'Dockerfile-preload'
# in this directory. You can pull the latest version of that image from the remote
# container repository associated with this project using the command 'pbdb pull api'.
# Alternatively, you can build it locally using the command 'pbdb build api preload'.
# See the file Dockerfile-preload for more information.
# 
# Once you have the preload image, you can build the Main API container image using
# the command 'pbdb build api'.

FROM paleomacro_pbapi_preload

EXPOSE 3000 3999

WORKDIR /var/paleomacro/pbdb-new/

# To build this container with the proper timezone setting, use --build-arg TZ=xxx
# where xxx specifies the timezone in which the server is located, for example
# "America/Chicago". The 'pbdb build' command will do this automatically. Without
# any argument the timezone will default to UTC, with no local time available. 

ARG TZ=Etc/UTC

RUN echo $TZ > /etc/timezone && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

COPY pbdb-new /var/paleomacro/pbdb-new/

CMD ["perl", "bin/data_service.pl"]

LABEL maintainer="mmcclenn@geology.wisc.edu"
LABEL version="1.0"
LABEL description="Paleobiology Database Main API"

LABEL buildcheck="bin/web_app.pl GET /data1.2/ | head -20"





