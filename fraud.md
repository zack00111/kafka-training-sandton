# Fraud producer, consumer, topic/s and event/s
The fraud consumer reads `PAYMENT_INITIATED` events from `payments.lifecycle`, scores them, and the fraud producer publishes `FRAUD_SCORE_LOW`, `FRAUD_SCORE_MEDIUM`, or `FRAUD_SCORE_HIGH` events to `fraud.payments.lifecycle`. The console commands below are smoke tests; application logic must perform the scoring and publish the resulting event.

## Topic configs
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic fraud.payments.lifecycle \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220752000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576

  ### Consumer configs

kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payments.lifecycle \
  --property print.key=true \
  --property key.separator=: \
  --group fraud-scoring \
  --consumer-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --consumer-property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --consumer-property max.poll.records=500 \
  --consumer-property session.timeout.ms=45000 \
  --consumer-property heartbeat.interval.ms=15000 \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.commit.interval.ms=5000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=10 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor


### Producer configs

kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic fraud.payments.lifecycle \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --producer-property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --producer-property acks=all \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=5 \
  --producer-property batch.size=32768 \
  --producer-property delivery.timeout.ms=120000 \
  --producer-property request.timeout.ms=30000