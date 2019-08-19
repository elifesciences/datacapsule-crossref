FROM python:3.7.4-buster

ENV PROJECT_FOLDER=/opt/sciencebeam-trainer-delft

ENV VENV=${PROJECT_HOME}/venv
RUN python3 -m venv ${VENV}
ENV PYTHONUSERBASE=${VENV} PATH=${VENV}/bin:$PATH

WORKDIR ${PROJECT_FOLDER}

ENV PATH=/root/.local/bin:${PATH}

COPY requirements.txt ./
RUN pip install -r requirements.txt

ARG install_dev
COPY requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install -r requirements.dev.txt; fi

COPY datacapsule_crossref ./datacapsule_crossref
