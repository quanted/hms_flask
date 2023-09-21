FROM mambaorg/micromamba:1.5.1-alpine

USER root
ARG APP_USER="hms"
RUN addgroup -S hms && adduser -S -G hms hms

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:/opt/conda/envs/env/bin:/opt/micromamba/bin:/opt/micromamba/envs/env/bin:$PATH

RUN apk add --upgrade apk-tools
RUN apk upgrade --available

RUN apk add wget bzip2 ca-certificates \
    py3-pip make sqlite gfortran git \
    mercurial subversion gdal geos

ARG CONDA_ENV="base"

COPY environment.yml /src/environment.yml
RUN micromamba install -n $CONDA_ENV -f /src/environment.yml

COPY uwsgi.ini /etc/uwsgi/
COPY . /src/hms_flask
RUN chmod 755 /src/hms_flask/start_flask.sh
WORKDIR /src/
EXPOSE 8080

ENV PYTHONPATH /src:/src/hms_flask/:$CONDA_ENV:$PYTHONPATH
ENV PATH /src:/src/hms_flask/:$CONDA_ENV:$PATH

# Security Issues Mitigations
# ------------------------- #
RUN apk del gfortran
RUN rm -R /opt/conda/pkgs/postgres*
RUN rm -R /opt/conda/bin/postgres*
RUN rm /home/www-data/pyenv/lib/python3.10/site-packages/tornado/test/test.key
# ------------------------- #

RUN chown -R $APP_USER:$APP_USER /src
USER $APP_USER

#ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["sh", "/src/hms_flask/start_flask.sh"]
