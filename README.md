## Витрина RFM-классификации пользователей, проверка качества данных.

| Задачи | Результат |
|:-----|------|
|1. Проверить качество исходных данных (пропуски, повторы, форматы, некорректные записи)     | Набором SQL запросов выполнил проверку качества исходных данных
|2. Создать витрину для RFM-классификации пользователей     |Создал витрину для RFM-классификации пользователей


## 1.1 Требования к целевой витрине.

1. Витрина должна распологаться в той же базе, в съеме analysis.
2. Витрина должна соержать следующие поля:
    - `user_id`
    - `recency`(число от 1 до 5)
    - `frequency`(число от 1 до 5)
    - `monetary_value`(число от 1 до 5)

3. Необходимы данные за 2022 год.
4. Витрина должна называться `dm_rfm_segments`.
5. Обновление витрины не требуется.

## 1.2 Анализ качества данных

### Таблица orderitems
Содержит позиции заказов.

Таблица содержит уникальный индекс `id`. Есть уникальный индекс из полей `order_id` + `product_id`. Все поля имеют ограничение `NOT NULL`. Есть необходимые внешние ключи на таблицы `orders`, `products`.

Цена должна быть строго больше нуля - проверяю запросом:
```sql
SELECT *
FROM orderitems
WHERE price=0
```
Таких строк нет.

Присутствует избыточность данных и возможные несоответствия наименований, так как содержит состав заказов с ценами и наименованиями позиций, которые также присутствуют в таблице products.

Присутствует внешний ключ по product_id на таблицу products, соответственно можно проверить запросом:
```sql
SELECT o.id, o.product_id, o."name", p.id, p."name", p.price
FROM orderitems o
LEFT JOIN products p ON o.product_id = p.id
WHERE (o."name" != p."name"
	   OR o.price != p.price
	   OR o."name" IS NULL
	   OR o.price IS NULL)
```
Разных и пустых значений нет.

Данная таблица для выполнения задания использоваться не будет.

### Таблица orders
Содержит данные о заказах: суммы, статус, ссылку на клиента. Поля:

`order_id` - уникальный код заказа
`order_ts` - дата/время заказа (поле типа timestamp)
`user_id` - код клиента
`payment` - сумма оплаты за заказ
`bonus_payment` - оплата бонусами, нужно изучить, добавлять или нет к сумме `payment`,
`cost` - общая сумма заказа?,
`bonus_grant` - начисленные бонусы за заказ, нужно изучить статистику
`status` - статус заказа

Содержит уникальный ключ `order_id`.

Все поля имеют ограничение `NOT NULL`.

Для полей paypment, bonus_payment, cost, bonus_grant нет ограничений на положительные значения. Проверяю их запросом:
```sql
SELECT t.bonus_payment, t.payment, t."cost", t.bonus_grant
FROM orders t
WHERE t.bonus_payment < 0
    OR t.payment <= 0
    OR t."cost" <= 0
    OR t.bonus_grant <= 0
```
Есть ограничение `CONSTRAINT orders_check CHECK ((cost = (payment + bonus_payment)))`, Значит cost - суммарная стоимость заказа для пользовтеля, нужно пользоватся ей.

Изучаю значения поля bonus_grant относительно суммы заказа:
```sql
select round(100.0*bonus_grant/cost, 1) percnt, count(1) cnt
from orders o
group by 100.0*bonus_grant/cost
```
|percnt| cnt |
|:-----|-----|
|1.0   |10000|

За каждый заказ начислено 1% бонусов от стоимости заказа.

Изучаю содержимое поля `status`. Ссылок на первичный ключ другой таблицы нет, но должна быть на справочник - таблицу `orderstatuses`. Проверяю статистику статусов следующим запросом:
```sql
select o.status order_status_id, os.id loookup_id, os."key", count(o.order_id)
from orders o
full join orderstatuses os on os.id = o.status
group by o.status, os.id, os."key"
order by o.status, os.id, os."key"
```
|order_status_id | loookup_id | key	| count|
|:---------------|------------|-----|------|
|4	             |  4 	      |Closed|	4991|
|5	|5	|Cancelled	|5009|
||1	|Open|0
||2	|Cooking|0
||3	|Delivering|0

У всех статус всех заказов согласно полю status: "Closed" или "Cancelled".

Проверяю, соответствует ли значение поля order_ts дате финального статуса заказа из таблицы `orderstatuslog`:
```sql
WITH t AS (
  SELECT osl.order_id,osl.status_id,
  	     last_value(osl.status_id) OVER
                    (PARTITION BY osl.order_id ORDER BY osl.dttm
				 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) log_last_status_id
  FROM orderstatuslog osl
) SELECT t.log_last_status_id,
        o.status order_status_id,
        count(1)
FROM t
LEFT JOIN orders o ON o.order_id = t.order_id
WHERE t.status_id = t.log_last_status_id
GROUP t.log_last_status_id, o.status
```
Последний по времени статус в таблице `orderstatuslog` и в таблице `orders` совпадают.

|log_last_status_id | order_status_id | count |
|:------------------|-----------------|-------|
|4	| 4	| 4991
|5	| 5	| 5009

На первом этапе буду использовать поле `status` таблицы `orders` как источник информации о финальном статусе заказа.

### Таблица orderstatuses

Таблица - справочник статусов заказов, два поля: уникальный ключ `id` и наименование в поле `key`.

Содержит уникальный индекс `id`.

На него ссылается внешний ключ по полю `status_id` из таблицы `orderstatuslog`.

Все поля имеют ограничение `NOT NULL`.

| id | key |
|:---|-----|
|1|	Open
|2|	Cooking
|3|	Delivering
|4|	Closed
|5|	Cancelled

Проверяю уникальность наименования - поле `key`, запросом
```sql
select "key", count(1)
from orderstatuses
group by "key"
having count(1) > 1
```
Повторяющихся значений нет.

### Таблица orderstatuslog

Таблица содержит последовательность смены статусов заказов, так как задано ограничение на уникальность `order_id, status_id`, есть внешние ссылки на таблицы orders и orderstatuses.

Имеется поле даты `dttm` в формате `TIMESTAMP`.

Все поля обязательны - имеют ограничение `NOT NULL`.

На первом этапе данную таблицу использовать не требуется.

### Таблица products

Является справочником составляющих заказа, так как на нее ссылается внешний ключ по полю `product_id` из таблицы `orderitems`.

Все поля имеют ограничение `NOT NULL`.

Поле `price` имеет ограничение - только положительные значения:
```sql
CONSTRAINT products_price_check CHECK ((price >= (0)::numeric))
```
Проверяю, есть ли составляющие с ценой = 0:
```sql
select count(1)
from products p
where p.price =0
```
Результат - 0 строк, такие составляющие отсутствуют.

Поле `price` имеет значение по умолчанию `(DEFAULT 0)`, что может привести к ошибке при заполнении.

Требуется проверить уникальность наименования - поле name, запросом
```sql
select p.name, count(1)
from products p
group by p."name"
having count(1) > 1
```
Повторяющихся значений нет.

### Таблица users
Содержит список клиентов, есть уникальный индекс id. Всего содержит 1000 записей.

Использую это число для контроля полноты расчитываемых метрик далее в анализе.

Для полей name и login нет проверки на уникальность, проверяю уникальность запросами:
```sql
SELECT u.login, count(1)
FROM users u
GROUP BY u.login
HAVING count(1) > 1;

SELECT u.name, count(1)
FROM users u
GROUP BY u."name"
HAVING count(1) > 1
```
Повторяющихся значений нет.

Для поля name нет ограничения на `NOT NULL`:

На таблицу не ссылаются внешние ключи, но очевидно поле `user_id` из `orders` должно иметь такую ссылку для поддержки целостности.
```sql
select t.user_id
from production.orders t
where t.user_id not in (select "id" from production.users)
```
Несоответствующих значений нет.

Данную таблицу буду использовать в задании.

В таблице users кроме отсуствия ограничений на `NULL` и уникальности полей, имеется несогласованность значений `name` и `login`: очевидно они принадлежат разным людям.

Возможно, что таблица неполная по количеству строк, так как в таблице не находится соответствий по ФИО и логинам даже в разных строках.

## 1.3 Подготовка витрины данных

Теперь, когда требования понятны, а исходные данные изучены, приступаю к реализации.

**1.4.1. Создаю VIEW для таблиц из базы `production`.**

При расчете витрины просят обращаться только к объектам из схемы `analysis`. `View` будут находиться в схеме `analysis` и вычитывать данные из схемы `production`.

Написал SQL-запросы для создания пяти `VIEW`(по одному на каждую таблицу).
```sql
DROP VIEW IF EXISTS analysis.orderitems ;
DROP VIEW IF EXISTS analysis.orders ;
DROP VIEW IF EXISTS analysis.orderstatuses ;
DROP VIEW IF EXISTS analysis.products ;
DROP VIEW IF EXISTS analysis.users ;
CREATE VIEW analysis.orderitems AS (SELECT * FROM production.orderitems);
CREATE VIEW analysis.orders AS (SELECT * FROM production.orders);
CREATE VIEW analysis.orderstatuses AS (SELECT * FROM production.orderstatuses);
CREATE VIEW analysis.products AS (SELECT * FROM production.products);
CREATE VIEW analysis.users AS (SELECT * FROM production.users);
```
**1.4.2. DDL-запрос для создания витрины.**

Далее необходимо создать витрину.

Создал витрину данных `dm_rfm_segments` с требуемыми колонками и ограничениями на них:

- `user_id`
- `recency`(число от 1 до 5)
- `frequency`(число от 1 до 5)
- `monetary_value`(число от 1 до 5)
```sql
DROP TABLE IF EXISTS analysis.dm_rfm_segments ;
CREATE TABLE analysis.dm_rfm_segments (
	user_id int4 NOT NULL,
 	recency int2 NOT NULL,
	frequency int2 NOT NULL,
	monetary_value int4 NOT NULL,
	CONSTRAINT dm_rfm_segments_pkey PRIMARY KEY (user_id),
	CONSTRAINT dm_rfm_segments_recency_check CHECK (recency >= 1 AND recency <= 5),
  	CONSTRAINT dm_rfm_segments_frequency_check CHECK (frequency >= 1 AND frequency <= 5),
  	CONSTRAINT dm_rfm_segments_monetary_value_check CHECK (monetary_value >= 1 AND monetary_value <= 5)
);
ALTER TABLE analysis.dm_rfm_segments
ADD CONSTRAINT dm_rfm_segments_user_id_fkey
FOREIGN KEY (user_id) REFERENCES production.users(id);
```

**1.4.3. SQL запрос для заполнения витрины.**
Далее реализовал расчет витрины на языке SQL и заполнил таблицу, созданную в предыдущем пункте.

Создал три вспомогательные таблицы для упрощения группировки
```sql
CREATE TABLE analysis.tmp_rfm_recency
(
    user_id INT NOT NULL PRIMARY KEY,
    recency INT NOT NULL CHECK(recency >= 1 AND recency <= 5)
);
CREATE TABLE analysis.tmp_rfm_frequency
(
    user_id INT NOT NULL PRIMARY KEY,
    frequency INT NOT NULL CHECK(frequency >= 1 AND frequency <= 5)
);
CREATE TABLE analysis.tmp_rfm_monetary_value
(
    user_id INT NOT NULL PRIMARY KEY,
    monetary_value INT NOT NULL CHECK(monetary_value >= 1 AND monetary_value <= 5)
);
```

И три запроса для заполнения созданных вспомогательных таблиц
```sql
DELETE FROM analysis.tmp_rfm_recency;

WITH t AS (
    SELECT u.id as user_id,
	        max(CASE WHEN oss."key" = 'Closed' THEN order_ts END) max_dttm,
	        sum(CASE WHEN oss."key" = 'Closed' THEN o.payment END) order_payment,
	        sum(CASE WHEN oss."key" = 'Closed' THEN 1 ELSE 0 END) order_count
    FROM analysis.users u
    LEFT JOIN analysis.orders o ON u.id = o.user_id
    LEFT JOIN analysis.orderstatuses oss ON oss.id = o.status
    GROUP BY u.id
), t2 AS (
    SELECT user_id,
            max_dttm,
            ntile(5) OVER (ORDER BY coalesce(max_dttm, '01/01/1990'::TIMESTAMP)) tile
	FROM t
) INSERT analysis.tmp_rfm_recency (
    SELECT user_id, tile
    FROM t2
);
```
```sql
DELETE FROM analysis.tmp_rfm_frequency;

WITH t AS (
    SELECT u.id AS user_id,
	       max(CASE WHEN oss."key" = 'Closed' THEN order_ts END) max_dttm,
	       sum(CASE WHEN oss."key" = 'Closed' THEN o.payment END) order_payment,
	       sum(CASE WHEN oss."key" = 'Closed' THEN 1 ELSE 0 END) order_count
    FROM analysis.users u
    LEFT JOIN analysis.orders o ON u.id = o.user_id
    LEFT JOIN analysis.orderstatuses oss ON oss.id = o.status
    GROUP BY u.id
), t2 AS (
    SELECT user_id,
           order_count,
           ntile(5) OVER (ORDER BY order_count) tile
	FROM t)
INSERT INTO analysis.tmp_rfm_frequency
(
    SELECT user_id, tile
    FROM t2
);
```
```sql
DELETE FROM analysis.tmp_rfm_monetary_value ;

WITH t AS
(
    SELECT u.id AS user_id,
	        max(case when oss."key" = 'Closed' then order_ts end) max_dttm,
	        sum(case when oss."key" = 'Closed' then o.payment end) order_payment,
	        sum(case when oss."key" = 'Closed' then 1 else 0 end) order_count,
    FROM analysis.users u
    LEFT JOIN analysis.orders o on u.id = o.user_id
    LEFT JOIN analysis.orderstatuses oss on oss.id = o.status
    GROUP BY u.id
), t2 AS (
    SEECT user_id,
            order_payment,
            ntile(5) over (order by order_payment asc) tile
	FROM t
) INSERT INTO analysis.tmp_rfm_monetary_value
(
    SEECT user_id, tile
    FROM t2
);
```
Финальный запрос для заполнения витрины данных
```sql
DELETE FROM analysis.dm_rfm_segments;
INSERT INTO analysis.dm_rfm_segments(user_id, recency, frequency, monetary_value)
(
    SELECT user_id,
           max(recency),
           max(frequency),
           max(monetary_value)
    FROM (SELECT t.user_id,
                 t.recency,
                 0::int2 AS frequency,
                 0::int2 AS monetary_value
          FROM analysis.tmp_rfm_recency  t
          UNION ALL
          SELECT trf.user_id, 0, trf.frequency, 0
          FROM analysis.tmp_rfm_frequency trf
          UNION ALL
          SELECT trmv.user_id, 0, 0, trmv.monetary_value
          FROM analysis.tmp_rfm_monetary_value trmv) t
    GROUP BY user_id
);
```
Первые десять строк из полученной таблицы, отсортированные по user_id с минимальными user_id.

|user_id|recency|frequency|monetary_value|
|:------|-------|---------|--------------|
|0	|1	|3	|4|
|1	|4	|3	|3|
|2	|2	|3	|5|
|3	|2	|4	|3|
|4	|4	|3	|3|
|5	|5	|5	|5|
|6	|1	|3	|5|
|7	|4	|3	|2|
|8	|1	|1	|3|
|9	|1	|2	|2|