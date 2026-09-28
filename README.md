# Restaurant Streaming Service — уведомления о промо в реальном времени

[![CI](https://github.com/uud-eparh/restaurant-streaming-service/actions/workflows/ci.yml/badge.svg)](https://github.com/uud-eparh/restaurant-streaming-service/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10-blue)
![Spark](https://img.shields.io/badge/Spark-3.3-orange)
![Kafka](https://img.shields.io/badge/Kafka-3.2-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)

## 🎯 Бизнес-задача

Агрегатор доставки еды запускает акции ресторанов. Нужно в реальном времени:
- Отслеживать новые акции из Kafka
- Находить подписчиков ресторана в PostgreSQL
- Отправлять им push-уведомления
- Сохранять результаты для аналитики

Ручная обработка занимала часы. Потоковый сервис обрабатывает события за секунды.

## 🏗️ Архитектура
``` text
┌─────────────────────────────────────────────────────────────┐
│ STREAMING PIPELINE │
├─────────────────────────────────────────────────────────────┤
│ │
│ ┌──────────┐ ┌───────────────────┐ ┌──────────────┐ │
│ │ Kafka │───▶│ Spark Structured │───▶│ PostgreSQL │ │
│ │ (input) │ │ Streaming │ │ (subscribers)│ │
│ └──────────┘ └───────────────────┘ └──────────────┘ │
│ │ │
│ ▼ │
│ ┌───────────────────┐ │
│ │ JOIN + FILTER │ │
│ │ (обогащение) │ │
│ └───────────────────┘ │
│ │ │
│ ▼ │
│ ┌──────────┐ ┌───────────────────┐ ┌──────────────┐ │
│ │ Kafka │◀───│ Notification │───▶│ PostgreSQL │ │
│ │ (output) │ │ Service │ │ (feedback) │ │
│ └──────────┘ └───────────────────┘ └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Компоненты системы:

| Компонент | Технология | Назначение |
|-----------|------------|------------|
| **Источник данных** | Kafka | Входящий поток акций ресторанов |
| **Обработка** | Spark Structured Streaming | Трансформация и обогащение данных |
| **Хранилище подписчиков** | PostgreSQL | Таблица избранных ресторанов пользователей |
| **Хранилище фидбэка** | PostgreSQL | Логирование отправленных уведомлений |
| **Выходной поток** | Kafka | Push-уведомления для сервиса |

## 🚀 Быстрый старт

### Предварительные требования:

- Docker и Docker Compose
- Доступ к Kafka кластеру
- PostgreSQL (локальный или удаленный)
- Spark 3.3.0

### Установка и запуск:

```bash
# 1. Клонировать репозиторий
git clone https://github.com/

# 2. Создать таблицы PostgreSQL

# 3. Запустить стриминг
python streaming_service.py
```

## 📊 Форматы сообщений

### Входное сообщение (в Kafka)

```json
{
    "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
    "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
    "adv_campaign_content": "first campaign",
    "adv_campaign_owner": "Ivanov Ivan Ivanovich",
    "adv_campaign_owner_contact": "iiivanov@restaurant.ru",
    "adv_campaign_datetime_start": 1659203516,
    "adv_campaign_datetime_end": 2659207116,
    "datetime_created": 1659131516
}
```

### Выходное сообщение (в Kafka)

```json
{
    "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
    "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
    "adv_campaign_content": "first campaign",
    "adv_campaign_owner": "Ivanov Ivan Ivanovich",
    "adv_campaign_owner_contact": "iiivanov@restaurant.ru",
    "adv_campaign_datetime_start": 1659203516,
    "adv_campaign_datetime_end": 2659207116,
    "client_id": "023e4567-e89b-12d3-a456-426614174000",
    "datetime_created": 1659131516,
    "trigger_datetime_created": 1659304828
}
```

## 🗄️ Структура базы данных

### Таблица подписчиков (входная)

```sql
CREATE TABLE public.subscribers_restaurants (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR NOT NULL,
    restaurant_id VARCHAR NOT NULL
);
```

### Таблица фидбэка (выходная)

```sql
CREATE TABLE public.subscribers_feedback (
    restaurant_id TEXT NOT NULL,
    adv_campaign_id TEXT NOT NULL,
    adv_campaign_content TEXT NOT NULL,
    adv_campaign_owner TEXT NOT NULL,
    adv_campaign_owner_contact TEXT NOT NULL,
    adv_campaign_datetime_start BIGINT NOT NULL,
    adv_campaign_datetime_end BIGINT NOT NULL,
    datetime_created BIGINT NOT NULL,
    client_id TEXT NOT NULL,
    trigger_datetime_created INTEGER NOT NULL,
    feedback VARCHAR NULL
);
```

## 🔄 ETL процессы

### Логика обработки одного сообщения:

| Шаг | Описание |
|-----|----------|
| 1 | **Чтение из Kafka** - потоковое чтение из топика `uud-eparh_in` |
| 2 | **Парсинг JSON** - преобразование в DataFrame согласно схеме |
| 3 | **Фильтрация** - только активные акции (текущее время между start и end) |
| 4 | **JOIN** - соединение с таблицей подписчиков по `restaurant_id` |
| 5 | **Кэширование** - сохранение результата в памяти |
| 6 | **Запись в PostgreSQL** - сохранение в `subscribers_feedback` |
| 7 | **Отправка в Kafka** - JSON в топик `uud-eparh_out` |
| 8 | **Очистка памяти** - удаление кэшированных данных |

## 📁 Структура проекта

```
restaurant-streaming/
├── streaming_service.py      # Основной скрипт стриминга
├── README.md                 # Документация
```

## 🧪 Тестирование

### Отправка тестового сообщения

```bash
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
         -X security.protocol=SASL_SSL \
         -X sasl.mechanisms=SCRAM-SHA-512 \
         -X sasl.username="de-student" \
         -X sasl.password="ltcneltyn" \
         -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
         -t uud-eparh_in \
         -K: \
         -P
```
```json
first_message:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant.ru","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
```
После ввода сообщения нажмите `Ctrl+D` для отправки.

### Проверка результатов


```bash
# Проверка выходного топика
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
         -t uud-eparh_out \
         -o -5 \
         -X security.protocol=SASL_SSL \
         -X sasl.mechanisms=SCRAM-SHA-512 \
         -X sasl.username="de-student" \
         -X sasl.password="ltcneltyn" \
         -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
         -C
```

## 🔧 Управление стримом

### Запуск

```bash
python streaming_service.py
```

### Остановка

```bash
# Ctrl+C в терминале
# Или в коде
query.stop()
```

## 👨‍💻 Автор

- **Username:** uud-eparh
- **Когорта:** 12
- **Проект:** Проектная работа по потоковой обработке данных

## 📄 Лицензия

Учебный проект
