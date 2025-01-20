  kubectl run kafka-admin -it \
  --namespace cloudera-kafka-demo \
  --image='container.repository.cloudera.com/cloudera/kafka:0.41.0.1.1.0-b79-kafka-3.7.0.1.1' \
  --rm=true \
  --restart=Never \
  --command -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --create \
  --topic ops-demo-topic


kubectl run kafka-producer -it \
  --namespace cloudera-kafka-demo \
  --image='container.repository.cloudera.com/cloudera/kafka:0.41.0.1.1.0-b79-kafka-3.7.0.1.1' \
  --rm=true \
  --restart=Never \
  --command -- /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
    --topic ops-demo-topic


kubectl run kafka-consumer -it \
  --namespace cloudera-kafka-demo \
  --image='container.repository.cloudera.com/cloudera/kafka:0.41.0.1.1.0-b79-kafka-3.7.0.1.1' \
  --rm=true \
  --restart=Never \
  --command -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
    --topic ops-demo-topic \
    --from-beginning
