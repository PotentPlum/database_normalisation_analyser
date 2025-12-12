FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y \
    MSSQL_PID=Developer \
    MSSQL_SA_PASSWORD=YourStrong!Passw0rd \
    MSSQL_COLLATION=SQL_Latin1_General_CP1_CI_AS

WORKDIR /usr/src/app

COPY operations_dataset.sql /usr/src/app/operations_dataset.sql
COPY init-db.sh /usr/src/app/init-db.sh

RUN chmod +x /usr/src/app/init-db.sh

EXPOSE 1433

CMD ["/bin/bash", "/usr/src/app/init-db.sh"]
