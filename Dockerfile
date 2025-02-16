FROM python:3.9-slim

WORKDIR /app
ADD requirements.txt /app/requirements.txt
# ADD requirements_extra.txt /app/requirements_extra.txt

RUN pip install -r requirements.txt
# RUN pip install -r requirements_extra.txt

ADD . /app/

RUN pip install .