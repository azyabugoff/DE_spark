# Working with Kafka

Внимание! 
* Для вас уже созданы топики для лабораторных работ. Создавать, удалять или изменять из не надо!
* Не используйте точку в названии топика. Используйте нижнее подчеркивание вместо нее (особенности кафки -- она не различает такие топики, как test.1 и test_1).

## Командная строка

### Настройки

Добавьте путь к Кафка в ~/.bashrc

export PATH=/usr/hdp/current/kafka-broker/bin:$PATH

Перед запуском команды кафки указывайте JAVA_HOME=/usr/jdk64/jdk1.8.0_112, например 

`JAVA_HOME=/usr/jdk64/jdk1.8.0_112 kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --topic lab04_input_data --from-beginning`

### Create topic

`kafka-topics.sh  --zookeeper localhost --create --partitions 1 --replication-factor 1 --topic test_4`

Такая схема предпочтительнее, если вы сами выбираете партиции:

`kafka-topics.sh  --zookeeper localhost --create --replica-assignment 1001 --topic test_4`

`kafka-topics.sh  --zookeeper localhost --create --replica-assignment 1001,1003 --topic test_4`

### Sending messages

`echo test1 | kafka-console-producer.sh --broker-list spark-master-1:6667 --topic test_1` 

### Recieving messages

`kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --topic test_1`

Заметьте, что опции сервера разные.

### list topics

`kafka-topics.sh --zookeeper localhost --list`

```
$ kafka-topics.sh --zookeeper localhost --describe --topic name_surname 
Topic:name_surname      PartitionCount:1        ReplicationFactor:1     Configs:
        Topic: name_surname     Partition: 0    Leader: 1001    Replicas: 1001  Isr: 1001
```

### alter topic (лучше не использовать)

--alter не сразу действует, требуется перезагрузка zookeeper и brokers.

### delete topic (лучше не использовать)

`kafka-topics.sh --zookeeper localhost --remove --topic test_4`

Однако, он реально тоже не удаляется, пока не перезагрузите zookeeper и brokers.
