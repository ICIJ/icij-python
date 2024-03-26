# We use placeholders here
FROM rabbitmq:3.12.0-management as rabbitmq
RUN echo "collect_statistics_interval = 100" >> /etc/rabbitmq/rabbitmq.conf
