FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y \
    MSSQL_PID=Developer \
    MSSQL_SA_PASSWORD=YourStrong!Passw0rd \
    MSSQL_COLLATION=SQL_Latin1_General_CP1_CI_AS

WORKDIR /usr/src/app

COPY --chmod=755 operations_dataset.sql sqlserver_3nf_audit.py init-db.sh ./

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && pip3 install --no-cache-dir sqlalchemy pymssql \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 1433

ENTRYPOINT ["/usr/src/app/init-db.sh"]
CMD ["serve"]
