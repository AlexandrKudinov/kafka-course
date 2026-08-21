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

## Практическая работа 3

Сервис обмена сообщениями на Kafka Streams.

### Логика

Входящие сообщения попадают в `messages`. Ключ сообщения — `recipientId`.
В `blocked_users` для каждого получателя хранится JSON со списком пользователей,
которых он заблокировал. Поток сообщений соединяется с этой таблицей и сообщения
от заблокированных отправителей отбрасываются.

Список блокировок материализуется в persistent state store:

```
blocked-users-store
```

Список запрещённых слов хранится в `banned_words`
Используется ключ `global`,
а значение содержит JSON-массив слов. Он читается как `GlobalKTable`, поэтому
обновление записи в Kafka автоматически меняет состояние, используемое обработчиками.

После проверки блокировки сообщение проходит цензуру. Запрещённые слова заменяются
на `***`, после чего сообщение отправляется в `filtered_messages`.


### Запуск

Поднимаем кластер :

```bash
docker compose up -d
```

Собираем проект:

```bash
./gradlew :practical-3:build
./gradlew :practical-3:bootRun
```

Kafka UI:

```text
http://localhost:8080
```

Для работы используются четыре Kafka topic:

| Topic | Partitions | Replication factor |
|---|---:|---:|
| `messages` | 1 | 3 |
| `filtered_messages` | 1 | 3 |
| `blocked_users` | 1 | 3 |
| `banned_words` | 1 | 3 |

Создаем топики:

```bash
docker exec -it kafka-1 kafka-topics   --create   --topic messages   --partitions 1   --replication-factor 3   --bootstrap-server kafka-1:19092
```

```bash
docker exec -it kafka-1 kafka-topics   --create   --topic filtered_messages   --partitions 1   --replication-factor 3   --bootstrap-server kafka-1:19092
```

```bash
docker exec -it kafka-1 kafka-topics   --create   --topic blocked_users   --partitions 1   --replication-factor 3   --bootstrap-server kafka-1:19092
```

```bash
docker exec -it kafka-1 kafka-topics   --create   --topic banned_words   --partitions 1   --replication-factor 3   --bootstrap-server kafka-1:19092
```

Проверить список созданных топиков:

```bash
docker exec -it kafka-1 kafka-topics   --list   --bootstrap-server kafka-1:19092
```


### Проверка работы приложения

Запустить тестовые данные:

```bash
../practical-3/scripts/test-data.sh
```

Проверка результата:

```bash
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:19092 \
  --topic filtered_messages \
  --from-beginning
```

Ожидается сообщение от `user-3`, например:

```json
{"id":"2","senderId":"user-3","recipientId":"user-1","text":"hello *** ***"}
```

Сообщение от `user-2` в `filtered_messages` отсутствует, потому что `user-2`
заблокирован пользователем `user-1`.

### Динамическое изменение цензуры

Обновить список запрещённых слов можно без перезапуска:

```bash
docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:19092 \
  --topic banned_words \
  --property parse.key=true \
  --property key.separator=: <<'EOF'
global:{"words":["secret"]}
```

# Практическая работа 5

Debezium Connector для PostgreSQL → Kafka и мониторинг Kafka Connect.

### Запуск

```bash
docker compose -f docker-compose-practical-5.yml up -d
```

 Проверить контейнеры:

```bash
docker compose -f docker-compose-practical-5.yml ps
```

### Debezium Connector

Зарегистрировать connector:

```bash
./practical-5/scripts/register-connector.sh
```

Проверить список connector-ов:

```bash
curl http://localhost:8083/connectors
```

Проверить статус:

```bash
curl http://localhost:8083/connectors/postgres-shop-connector/status
```

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:29092
```

## Тестовые данные

Таблицы создаются автоматически из `postgres/init.sql`. Дополнительные изменения:

```bash
docker exec -i postgres psql -U postgres -d shop < practical-5/scripts/test-data.sql
```

### Чтение CDC через Java

```bash
./gradlew :practical-5:bootRun
```

Приложение читает оба Debezium topic и выводит события через SLF4J.

### Тестовая запись в orders

```bash
docker exec -it postgres psql -U postgres -d shop -c "INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Test Product', 5);"
```

 Проверка топика orders

```bash
docker exec -it kafka kafka-console-consumer \
--bootstrap-server kafka:29092 \
--topic practical5.public.orders \
--from-beginning
```

## Проверка

1. `docker compose ... up -d`.
2. `curl http://localhost:8083/connectors/postgres-shop-connector/status` → `RUNNING`.
3. Добавить/изменить строки в PostgreSQL.
4. Проверить `practical5.public.users` и `practical5.public.orders`.
5. Запустить Java consumer и увидеть CDC events в логах.
6. Открыть Prometheus и Grafana и проверить метрики Kafka Connect.


