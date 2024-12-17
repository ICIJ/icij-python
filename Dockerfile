# We use placeholders here
FROM rabbitmq:3.13.7-management as rabbitmq
RUN echo "collect_statistics_interval = 100" >> /etc/rabbitmq/rabbitmq.conf
RUN echo "[{rabbit, [ {channel_tick_interval, 1000} ]}]." >> /etc/rabbitmq/advanced.config
