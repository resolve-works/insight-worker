FROM jbarlow83/ocrmypdf-alpine:v16.9.0

# TODO - move the building / installing to builder step

RUN apk update
RUN apk add libmagic py3-pip

COPY . /home/insight
WORKDIR /home/insight
RUN pip install . --break-system-packages
RUN rm -rf /home/insight

WORKDIR /home
ENTRYPOINT []
CMD insight-worker process-messages

