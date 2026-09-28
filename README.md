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

    Kafka (input) → Spark Structured Streaming → PostgreSQL (subscribers)
                            ↓
                     JOIN + FILTER (обогащение)
                            ↓
    Kafka (output) ← Notification Service → PostgreSQL (feedback)

## 🔄 Как работает

1. **Чтение из Kafka** — потоковое чтение из топика `restaurant_promos_in`
2. **Парсинг JSON** — преобразование в DataFrame согласно схеме
3. **Фильтрация** — только активные акции (текущее время между start и end)
4. **JOIN** — соединение с таблицей подписчиков по `restaurant_id`
5. **Кэширование** — сохранение результата в памяти
6. **Запись в PostgreSQL** — сохранение в `subscribers_feedback`
7. **Отправка в Kafka** — JSON в топик `restaurant_promos_out`
8. **Очистка памяти** — удаление кэшированных данных

## 📊 Форматы сообщений

### Входное сообщение (Kafka)

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

### Выходное сообщение (Kafka)

    {
        "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
        "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
        "adv_campaign_content": "first campaign",
        "client_id": "023e4567-e89b-12d3-a456-426614174000",
        "trigger_datetime_created": 1659304828
    }

## 🗄️ Структура базы данных

### Таблица подписчиков (входная)

    CREATE TABLE public.subscribers_restaurants (
        id SERIAL PRIMARY KEY,
        client_id VARCHAR NOT NULL,
        restaurant_id VARCHAR NOT NULL
    );

### Таблица фидбэка (выходная)

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

## 🚀 Быстрый старт

### Требования

- Docker и Docker Compose
- Доступ к Kafka кластеру
- PostgreSQL (локальный или удалённый)
- Spark 3.3.0

### Установка

    git clone https://github.com/uud-eparh/restaurant-streaming-service.git
    cd restaurant-streaming-service

    python -m venv .venv
    source .venv/bin/activate

    pip install -r requirements.txt

    cp .env.example .env

    psql -h localhost -U postgres -d postgres -f sql/create_tables.sql

    python streaming_service.py

### Тестирование

    kafkacat -b $KAFKA_HOST:$KAFKA_PORT \
      -X security.protocol=SASL_SSL \
      -X sasl.mechanisms=SCRAM-SHA-512 \
      -X sasl.username=$KAFKA_USER \
      -X sasl.password=$KAFKA_PASSWORD \
      -t restaurant_promos_in \
      -P

    kafkacat -b $KAFKA_HOST:$KAFKA_PORT \
      -t restaurant_promos_out \
      -o -5 \
      -C

## 🛠️ Стек

| Компонент | Технология | Назначение |
| :--- | :--- | :--- |
| Обработка | Spark Structured Streaming | Потоковая обработка |
| Брокер | Apache Kafka | Передача событий |
| Хранилище | PostgreSQL | Подписчики и фидбэк |
| Язык | Python 3.10 (PySpark) | Разработка сервиса |
| Инфраструктура | Docker | Контейнеризация |

## 📈 Результаты

| Показатель | Значение |
| :--- | :--- |
| Задержка обработки | Секунды |
| Пропускная способность | До 1000 событий/сек |
| Дедупликация | Автоматическая |

## 📄 Лицензия

MIT