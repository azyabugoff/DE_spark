
# PostgreSQL

Для вас развернут инстанс PostgreSQL на 10.0.0.5:5432. В нем для каждого созданы базы данных `name_surname`, и аккаунты `name_surname` с таким же паролем, как и в ЛК.

## Создание таблицы

Воспользуйтесь утилитой `psql`.

Логин в базу `artem_trunov` под именем `artem_trunov`:

`$ psql -h 10.0.0.5 -U artem_trunov artem_trunov`

Введите пароль.

Создайте таблицу:
```
artem_trunov=> create table clients (uid VARCHAR (40) UNIQUE NOT NULL, ... );
CREATE TABLE
```

Дайте доступ пользователю labchecker2:

`GRANT SELECT ON TABLE clients TO labchecker2;`

## Доступ из Spark

Осуществляется через JDBC драйвер PostgreSQL.

### Запись

Обратите внимание, что таким образом и создается таблица

```
res.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.5:5432/kirill_likhouzov")
    .option("dbtable", "clients")
    .option("user", "kirill_likhouzov")
    .option("password", pwd)
    .option("driver", "org.postgresql.Driver")
    .save()
```



## Полезные команды:

* \dt - список таблиц
* \q или  Ctrl-D - Выход

## Ссылки

* https://tyvik.ru/posts/postgresql-psql/
* https://spark.apache.org/docs/2.4.4/sql-data-sources-jdbc.html
