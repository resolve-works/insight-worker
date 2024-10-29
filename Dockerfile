FROM jbarlow83/ocrmypdf

# TODO - move the building / installing to builder step

RUN apt update && apt install -y tesseract-ocr-nld libmagic1 python3-pip

COPY . /home/insight
WORKDIR /home/insight
RUN pip install . --break-system-packages
RUN rm -rf /home/insight

WORKDIR /home
ENTRYPOINT []
CMD insight-worker process-messages

