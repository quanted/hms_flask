FROM continuumio/miniconda3:4.10.3p0-alpine as base

ARG GDAL_VERSION=3.4.1
ARG CONDA_ENV_BASE=pyenv

RUN apk update
RUN apk upgrade
RUN apk add --no-cache geos gdal cmake git gfortran sqlite sqlite-dev
RUN pip install -U pip

COPY requirements.txt /tmp/requirements.txt

RUN conda config --add channels conda-forge
RUN conda create -n $CONDA_ENV_BASE python=3.9.10 gdal=$GDAL_VERSION
RUN conda install -n $CONDA_ENV_BASE --file /tmp/requirements.txt
RUN conda install -n $CONDA_ENV_BASE uwsgi
RUN conda install --force-reinstall -n $CONDA_ENV_BASE fiona
RUN conda install --force-reinstall -n $CONDA_ENV_BASE geopandas

RUN conda run -n $CONDA_ENV_BASE --no-capture-output conda clean -acfy && \
    find /opt/conda -follow -type f -name '*.a' -delete && \
    find /opt/conda -follow -type f -name '*.pyc' -delete && \
    find /opt/conda -follow -type f -name '*.js.map' -delete

# Main image build
FROM continuumio/miniconda3:4.10.3p0-alpine as prime

ENV APP_USER=www-data
ENV CONDA_ENV=/home/www-data/pyenv

RUN adduser -S $APP_USER -G $APP_USER

RUN apk update
RUN apk upgrade
RUN apk add --no-cache geos gdal cmake git gfortran sqlite sqlite-dev

COPY uwsgi.ini /etc/uwsgi/
COPY . /src/hms_flask
RUN chmod 755 /src/hms_flask/start_flask.sh
WORKDIR /src/
EXPOSE 8080

COPY --from=base /opt/conda/envs/pyenv $CONDA_ENV

ENV PYTHONPATH /src:/src/hms_flask/:$CONDA_ENV:$PYTHONPATH
ENV PATH /src:/src/hms_flask/:$CONDA_ENV:$PATH

RUN chown -R $APP_USER:$APP_USER /src
RUN chown $APP_USER:$APP_USER $CONDA_ENV
USER $APP_USER

CMD ["conda", "run", "-p", "$CONDA_ENV", "--no-capture-output", "sh", "/src/hms_flask/start_flask.sh"]
