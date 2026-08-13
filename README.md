# Практические работы по Apache Kafka


## Практическая работа 2

В модуле реализованы один producer и два consumer-а для Kafka.

### Подготовка перед запуском:

```bash
gradle wrapper --gradle-version 8.10.2
```

### Запуск Kafka кластера

```bash
docker compose up -d
```

Kafka UI -> http://localhost:8080


### Создание topic:

```bash
docker exec -it kafka-1 kafka-topics   --create   --topic test-topic   --partitions 3   --replication-factor 2   --bootstrap-server kafka-1:19092
```

Проверка:

```bash
docker exec -it kafka-1 kafka-topics   --describe   --topic test-topic   --bootstrap-server kafka-1:19092
```

### Запуск приложения:

Сборка:

```bash
./gradlew :practical-2:build
```
Запуск:

```bash
./gradlew :practical-2:bootRun
```


### Consumer groups

```text
single-consumer-group
batch-consumer-group
```

Разные группы получают одни и те же сообщения независимо друг от друга.

### Проверка групп

```bash
docker exec -it kafka-1 kafka-consumer-groups   --bootstrap-server kafka-1:19092   --list
```

