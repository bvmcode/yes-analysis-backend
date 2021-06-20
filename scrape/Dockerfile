FROM ubuntu:latest

RUN apt-get update \
  && apt-get install -y python3-pip python3-dev libpq-dev unixodbc-dev libsasl2-dev libsasl2-modules libssl-dev libffi-dev freetds-dev freetds-bin tdsodbc\
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip 

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN python3 -m pip install -r requirements.txt

COPY . /app

CMD ["python", "-u", "main.py"]