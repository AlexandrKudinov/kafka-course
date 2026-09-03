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

| Topic               | Partitions | Replication factor |
|---------------------|-----------:|-------------------:|
| `messages`          |          1 |                  3 |
| `filtered_messages` |          1 |                  3 |
| `blocked_users`     |          1 |                  3 |
| `banned_words`      |          1 |                  3 |

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
{
  "id": "2",
  "senderId": "user-3",
  "recipientId": "user-1",
  "text": "hello *** ***"
}
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

### Проверка

1. `docker compose ... up -d`.
2. `curl http://localhost:8083/connectors/postgres-shop-connector/status` → `RUNNING`.
3. Добавить/изменить строки в PostgreSQL.
4. Проверить `practical5.public.users` и `practical5.public.orders`.
5. Запустить Java consumer и увидеть CDC events в логах.
6. Открыть Prometheus и Grafana и проверить метрики Kafka Connect.

## Практическая работа 6

Настройка защищённого SSL-соединения и управление доступом в Apache Kafka.

Практическая работа 6 использует отдельный кластер из ZooKeeper и трёх Kafka broker.
Все подключения к Kafka выполняются по SSL с обязательной клиентской аутентификацией.
Доступ к топикам ограничивается Kafka ACL.

### Запуск кластера

```bash
docker compose -f docker-compose-practical-6.yml up -d
```

Проверка:

```bash
docker compose -f docker-compose-practical-6.yml ps
```

Должны работать:

```text
practical6-zookeeper
practical6-kafka-0
practical6-kafka-1
practical6-kafka-2
```

Kafka broker должны получить статус `healthy`.

### Проверка SSL

```bash
docker exec practical6-kafka-0 \
  kafka-broker-api-versions \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /tmp/admin.properties
```

Если команда возвращает список API broker, admin-клиент успешно подключился к Kafka по SSL.

### Создание топиков и ACL

После запуска всех broker:

```bash
./practical-6/scripts/create-topics-and-acls.sh
```

Создаются:

```text
topic-1
topic-2
```

Оба топика имеют 3 partition и replication factor 3.

Проверка:

```bash
docker exec practical6-kafka-0 kafka-topics \
  --list \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /tmp/admin.properties
```

Подробная информация:

```bash
docker exec practical6-kafka-0 kafka-topics \
  --describe --topic topic-1 \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /tmp/admin.properties
```

Аналогично для `topic-2`. Команды также находятся в `practical-6/topic.txt`.

### ACL

| Principal  | topic-1         | topic-2          |
|------------|-----------------|------------------|
| `producer` | WRITE, DESCRIBE | WRITE, DESCRIBE  |
| `consumer` | READ, DESCRIBE  | READ отсутствует |
| `admin`    | полный доступ   | полный доступ    |

Для consumer дополнительно разрешена consumer group `practical-6-consumer`.

Проверка ACL:

```bash
docker exec practical6-kafka-0 kafka-acls \
  --list \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /tmp/admin.properties
```

### Проверки доступов

```bash
./practical-6/scripts/test-access.sh
```

Ожидаемый результат:

```text
topic-1 producer       -> успешно
topic-1 consumer       -> успешно

topic-2 producer       -> успешно
topic-2 consumer       -> AuthorizationException / TopicAuthorizationException
```

### Проверка

```
После выполнения:

- ZooKeeper работает;
- 3 broker работают по SSL;
- `topic-1` доступен producer и consumer;
- `topic-2` доступен producer, но недоступен consumer;
- сертификаты и keystore/truststore находятся в репозитории;
- Java producer/consumer настроены на SSL.
```

# Практическая работа 7

Практическая работа состоит из двух частей:

1. Развёртывание и настройка Kafka-кластера.
2. Интеграция Kafka с Apache NiFi.

## 1. Локальная инфраструктура

Запуск из корня проекта:

```bash
docker compose -f docker-compose-practical-7.yml up -d
```

Проверка:

```bash
docker compose -f docker-compose-practical-7.yml ps
```

Проверка Kafka:

```bash
docker exec practical7-kafka-0 \
  kafka-broker-api-versions \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092
```

Schema Registry:

```bash
curl http://localhost:8081/subjects
```

NiFi:

```text
https://localhost:8443/nifi
```

## 2. Создание топика и схемы

```bash
./practical-7/scripts/create-topic-and-schema.sh
```

Проверка:

```bash
docker exec practical7-kafka-0 kafka-topics \
  --describe \
  --topic practical7-messages \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092
```

Проверка Schema Registry:

```bash
curl http://localhost:8081/subjects
```

## 3. Producer

Собрать проект:

```bash
./gradlew :practical-7:build
```

Запустить producer:

```bash
./gradlew :practical-7:bootRun --args='produce'
```

Producer отправляет 10 JSON-сообщений в `practical7-messages`.

Проверить сообщения:

```bash
docker exec -it practical7-kafka-0 kafka-console-consumer \
  --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
  --topic practical7-messages \
  --from-beginning
```

## 4. Consumer

В отдельном терминале:

```bash
./gradlew :practical-7:bootRun --args='consume'
```

Consumer читает сообщения и выводит:

```text
Received: topic=practical7-messages partition=... offset=... value=...
```

## 5. Построение схемы в NiFi

вручную

cкрин UI NiFi с настройкой вычитки
./practical-7/src/main/resources/img.png

# Финальный проект

## JSON-хранилище

Для проекта используется один набор исходных данных:

```text
final-project/data/products.json
```

Он используется SHOP API и CLIENT API

## Топики

Основной кластер:

| Topic | Partitions | RF | Назначение |
|---|---:|---:|---|
| `shop-products` | 3 | 3 | товары от SHOP API |
| `forbidden-products` | 1 | 3 | состояние запрещённых товаров |
| `filtered-products` | 3 | 3 | товары после фильтрации |
| `client-requests` | 3 | 3 | события поиска CLIENT API |
| `recommendations` | 3 | 3 | готовые рекомендации |

## Запуск всей инфраструктуры

```bash
docker compose -f docker-compose-final-project.yml up -d
```

## Проверка основного Kafka-кластера и SSL

Проверить API broker:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 \
  kafka-broker-api-versions \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /etc/kafka/client-config/admin.properties
```

## Создание топиков

```bash
./final-project/scripts/create-topics.sh
```

Проверка:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 kafka-topics \
  --list \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /etc/kafka/client-config/admin.properties
```

Для проверки RF:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 kafka-topics \
  --describe --topic shop-products \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /etc/kafka/client-config/admin.properties
```

Ожидается `ReplicationFactor: 3`.

## Создание ACL

```bash
./final-project/scripts/create-acls.sh
```

Проверка:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 kafka-acls \
  --list \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --command-config /etc/kafka/client-config/admin.properties
```

Основные права:

- `shop` → WRITE в `shop-products`;
- `streams` → READ `shop-products`, READ/WRITE `forbidden-products`, WRITE `filtered-products`;
- `client` → WRITE `client-requests`, READ `recommendations`;
- `analytics` → READ `client-requests`, WRITE `recommendations`;
- `streams` → READ `filtered-products` для replication group;
- `connect` — super user для Kafka Connect.

## Запуск Kafka Streams

В отдельном терминале:

```bash
./gradlew :final-project:bootRun --args='stream'
```
Topology использует state store:

```text
forbidden-products-store
```

Добавить товар в запрещённые:

```bash
./gradlew :final-project:bootRun --args='forbid p-003'
```

Удалить товар из списка:

```bash
./gradlew :final-project:bootRun --args='allow p-003'
```

## Проверка SHOP API и фильтрации

В новом терминале:

```bash
./gradlew :final-project:bootRun --args='shop'
```

Проверить входной поток:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 kafka-console-consumer \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --topic shop-products --from-beginning \
  --consumer.config /etc/kafka/client-config/admin.properties
```

В другом терминале проверить результат:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 kafka-console-consumer \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --topic filtered-products --from-beginning \
  --consumer.config /etc/kafka/client-config/admin.properties
```

После команды `forbid p-003` новые сообщения для `p-003` не должны проходить в `filtered-products`.

## Проверка репликации во второй кластер

Запустить:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 \
kafka-acls \
--bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
--command-config /etc/kafka/client-config/admin.properties \
--add \
--allow-principal User:streams \
--operation READ \
--operation DESCRIBE \
--topic filtered-products
```

```bash
./gradlew :final-project:bootRun --args='replicate'
```

Оставьте процесс работающим.

Проверить второй кластер:

```bash
docker compose -f docker-compose-final-project.yml exec -T secondary-kafka-0 \
  kafka-topics --list \
  --bootstrap-server secondary-kafka-0:9092,secondary-kafka-1:9092,secondary-kafka-2:9092
```

Прочитать реплицированные данные:

```bash
docker compose -f docker-compose-final-project.yml exec -T secondary-kafka-0 kafka-console-consumer \
  --bootstrap-server secondary-kafka-0:9092,secondary-kafka-1:9092,secondary-kafka-2:9092 \
  --topic filtered-products --from-beginning
```

## Аналитика

Запустить:

```bash
./gradlew :final-project:bootRun --args='analytics'
```

Сервис работает как consumer двух потоков:

- `filtered-products` из второго Kafka-кластера;
- `client-requests` из основного Kafka-кластера.

Для поиска пользователь отправляет запрос:

```bash
./gradlew :final-project:bootRun --args='search electronics'
```

CLIENT API сначала читает `data/products.json`, 
поэтому поиск выполняется в том же хранилище данных. 
Затем событие поиска публикуется в `client-requests`.

Analytics получает событие и создаёт готовую рекомендацию в `recommendations`.

Проверка:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 kafka-console-consumer \
  --bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
  --topic recommendations --from-beginning \
  --consumer.config /etc/kafka/client-config/admin.properties
```

## Получение персонализированной рекомендации CLIENT API

После того как Analytics сформировал рекомендации:

```bash
docker compose -f docker-compose-final-project.yml exec -T kafka-0 \
kafka-acls \
--bootstrap-server kafka-0:9093,kafka-1:9093,kafka-2:9093 \
--command-config /etc/kafka/client-config/admin.properties \
--add \
--allow-principal User:recommend-electronics \
--operation READ \
--operation DESCRIBE \
--group client-recommendations- \
--resource-pattern-type PREFIXED
```

```bash
./gradlew :final-project:bootRun --args='recommend-electronics'
```

В этом сценарии CLIENT API **не отправляет сообщение в Kafka**. 
Он только читает готовые данные из `recommendations`.

Таким образом:

```text
SEARCH:
CLIENT API -> client-requests -> Analytics -> recommendations
    producer                         producer

RECOMMEND:
recommendations -> CLIENT API
consumer             consumer
```

## Kafka Connect

Зарегистрировать FileStreamSink:

```bash
./final-project/scripts/register-file-sink.sh
```

Проверить статус:

```bash
curl http://localhost:8084/connectors/final-file-sink/status
```

Ожидается состояние `RUNNING`.

После появления сообщений в `filtered-products` проверить:

```bash
cat final-project/storage/filtered-products.jsonl
```

В файл должны попадать только сообщения из `filtered-products`.

## Проверка мониторинга

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

Alertmanager:

```text
http://localhost:9093
```

в Prometheus видим targets:

```text
jmx-kafka-0
jmx-kafka-1
jmx-kafka-2
jmx-connect
```

Проверка:

```bash
curl http://localhost:9090/api/v1/targets
```

Проверить метрики exporter:

```bash
curl http://localhost:5556/metrics | head
```

Grafana автоматически получает datasource Prometheus и dashboard из `final-project/grafana`.

##  Проверка ACL

```bash
./final-project/scripts/test-acls.sh
```

Ожидается:

```text
shop -> shop-products: WRITE succeed
client -> shop-products: WRITE fail
OK: client is denied WRITE access
```

##  Общая автоматическая проверка

После запуска инфраструктуры:

```bash
./final-project/scripts/check-final-project.sh
```

Проверяются:

- состояние контейнеров;
- основной Kafka cluster;
- второй Kafka cluster;
- Kafka Connect;
- Prometheus;
- Grafana.