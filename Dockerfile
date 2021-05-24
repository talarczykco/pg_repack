FROM  ubuntu

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

ENTRYPOINT ["/usr/lib/postgresql/11/bin/pg_repack", "-k"]
