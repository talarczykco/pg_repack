FROM  ubuntu

# configure timezone so process doesn't hang on interactive selection
ENV   DEBIAN_FRONTEND=noninteractive
ENV   TZ=America/New_York
RUN   ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN   apt-get update
RUN   apt-get install -y lsb-core python3-pip wget
RUN   wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN   echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`"-pgdg main | tee /etc/apt/sources.list.d/pgdg.list
RUN   apt-get update
RUN   apt-get install -y libssl-dev postgresql-server-dev-11
RUN   pip3 install pgxnclient
RUN   pgxn install pg_repack=1.4.4
# RUN   /usr/lib/postgresql/11/bin/pg_repack

# root@0bda2bbdd331:/# /usr/lib/postgresql/11/bin/pg_repack -h docker.for.mac.localhost -U postgres tfdev1
# ERROR: pg_repack failed with error: You must be a superuser to use pg_repack
