# get shiny serves plus tidyverse packages image
FROM rocker/shiny-verse:4.2.2
# system libraries of general use
RUN apt-get update && apt-get install -y \
  gsfonts \
  imagemagick \
  sudo \
  make \
  pandoc \
  pandoc-citeproc \
  libglpk-dev \
  libgmp3-dev \
  libicu-dev \
  libxml2-dev \
  libmagick++-dev \
  libcurl4-gnutls-dev \
  libcairo2-dev \
  libxt-dev \
  libssl-dev \
  libssh2-1-dev \
  libudunits2-dev \
  libgdal-dev \
  libgeos-dev \
  libproj-dev \
  libmysqlclient-dev \
  zlib1g-dev

RUN apt-get install -y curl wget gnupg2
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
  unixodbc \
  unixodbc-dev \
  freetds-dev \
  freetds-bin \
  tdsodbc \
  msodbcsql17

RUN echo "[FreeTDS]\n\
  Description = FreeTDS unixODBC Driver\n\
  Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
  Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
  FileUsage = 1" >> /etc/odbcinst.ini

RUN echo "[SQL Server]\n\
  Description = Microsoft SQL Server ODBC Driver V17 for Linux\n\
  Driver = /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.2.1\n\
  UsageCount = 1" >> /etc/odbcinst.ini

RUN apt-get update && apt-get install -y \
  libsqliteodbc \
  odbc-postgresql \
  libicu-dev

COPY shiny_renv.lock renv.lock 
RUN Rscript -e "install.packages('renv')"
RUN Rscript -e "renv::restore()"

ENV DATABASE_NAME="work" \
  PWD_DATABASE="Mqnj65hn!65jk@" \
  SERVER_NAME="192.168.0.223" \
  UID_DATABASE="ura" 

# copy the app to the image
COPY . /srv/shiny-server/ 
WORKDIR /srv/shiny-server/
# select port
EXPOSE 3838
# allow permission
RUN sudo chown -R shiny:shiny /srv/shiny-server
RUN sudo locale-gen uk_UA.UTF-8

# run app 
CMD ["R", "-e", "shiny::runApp('app/app.R', host = '0.0.0.0', port = 3838)"]