FROM jbarlow83/ocrmypdf

RUN apt update && apt install -y tesseract-ocr-nld

COPY . /home/insight
WORKDIR /home/insight
RUN pip install .
RUN rm -rf /home/insight

WORKDIR /home
ENTRYPOINT []
CMD insight-worker process-messages

