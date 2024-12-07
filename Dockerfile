FROM jbarlow83/ocrmypdf:v16.6.1

# TODO - move the building / installing to builder step

RUN apt update
RUN apt install -y libmagic1 python3-pip

COPY . /home/insight
WORKDIR /home/insight
RUN pip install .
RUN rm -rf /home/insight

WORKDIR /home
ENTRYPOINT []
CMD insight-worker process-messages

