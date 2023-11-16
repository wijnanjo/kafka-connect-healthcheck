FROM python:3.12-alpine

# params controlling the RESTART of a connector which is in status FAILED
# technically it is a connector's TASK that we restart
# uses an exponential backoff period

# select which connectors will be restart. Leave empty for no restart behavior
ENV RESTART_CONNECTORS_REGEX=""
# restart on this error
ENV RESTART_ERROR_REGEX=""
# restart backoff params
ENV STOP_RESTARTING_AFTER_SECONDS=180
ENV INITIAL_RESTART_DELAY_SECONDS=10

COPY setup.py README.md LICENSE /kafka-connect-healthcheck/
COPY kafka_connect_healthcheck/ /kafka-connect-healthcheck/kafka_connect_healthcheck/

RUN cd /kafka-connect-healthcheck && pip3 install -e .

CMD ["kafka-connect-healthcheck"]