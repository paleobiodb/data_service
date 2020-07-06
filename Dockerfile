# 
# Paleobiology Database - main API image
# 
# The image 'paleobiodb_api_preload' can be built using the file 'Dockerfile-preload'.
# See that file for more information.

FROM paleobiodb_pbapi_preload

COPY pbdb-new /var/paleomacro/pbdb-new/

VOLUME /var/paleomacro/pbdb-new/logs

EXPOSE 3000 3999

WORKDIR /var/paleomacro/pbdb-new/

CMD perl bin/data_service.pl

LABEL maintainer="mmcclenn@geology.wisc.edu"
LABEL version="1.0"
LABEL description="Paleobiology Database Main API"

LABEL buildcheck="bin/web_app.pl GET /data1.2/ | head -20"





