FROM continuumio/miniconda3:4.10.3

RUN apt-get update --allow-releaseinfo-change -y
RUN apt-get upgrade --fix-missing -y
RUN apt-get install -y --fix-missing --no-install-recommends \
    python3-pip software-properties-common build-essential \
    cmake git sqlite3 gfortran python-dev && \
    pip install -U pip

COPY uwsgi.ini /etc/uwsgi/
COPY . /src/hms_flask
RUN chmod 755 /src/hms_flask/start_flask.sh
WORKDIR /src/
EXPOSE 8080

RUN conda create --name pyenv python=3.9
RUN conda config --add channels conda-forge
RUN conda run -n pyenv --no-capture-output conda install --file /src/hms_flask/requirements.txt
RUN conda install -n pyenv uwsgi

ENV PYTHONPATH /src:/src/hms_flask/:$PYTHONPATH
ENV PATH /src:/src/hms_flask/:$PATH

#RUN chmod 755 /src/hms_flask/start_flask.sh
CMD ["conda", "run", "-n", "pyenv", "--no-capture-output", "sh", "/src/hms_flask/start_flask.sh"]