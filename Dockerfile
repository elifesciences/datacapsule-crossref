FROM python:3.7.4-buster

ENV PROJECT_FOLDER=/opt/sciencebeam-trainer-delft

WORKDIR ${PROJECT_FOLDER}

ENV PATH=/root/.local/bin:${PATH}

COPY requirements.txt ./
RUN pip install --user -r requirements.txt

ARG install_dev
COPY requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install -r requirements.dev.txt; fi

COPY datacapsule_crossref ./datacapsule_crossref
