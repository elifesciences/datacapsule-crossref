FROM python:3.7.4-buster

ENV PROJECT_FOLDER=/opt/sciencebeam-trainer-delft

WORKDIR ${PROJECT_FOLDER}

ENV PATH=/root/.local/bin:${PATH}

COPY requirements.txt ./
RUN pip install --user -r requirements.txt
