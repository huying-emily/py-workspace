FROM 051694948699.dkr.ecr.ap-southeast-1.amazonaws.com/python-ds-stack:latest

LABEL maintainer=""

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.1.5


# add SSH deploy key
RUN mkdir /root/.ssh
COPY .ssh/id_rsa /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa

RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan -t rsa gitlab.com >> /root/.ssh/known_hosts
RUN ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

RUN pip install "poetry==$POETRY_VERSION"

COPY . .
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction

RUN rm -rf /root/.ssh

CMD python manage.py
