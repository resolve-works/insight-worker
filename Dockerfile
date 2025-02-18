# We're not using the alpine image because we'd have to compile wheels for arm64 musl C
FROM jbarlow83/ocrmypdf:v16.9.0

RUN apt update
RUN apt install -y libmagic1 python3-pip

COPY . /home/insight
WORKDIR /home/insight
RUN pip install . --break-system-packages
RUN rm -rf /home/insight

WORKDIR /home
ENTRYPOINT []
CMD insight-worker process-messages

