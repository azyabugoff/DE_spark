# Elasticsearch API

## Аутентификация

Используется простая HTTP-аутентификация по имени пользователя и паролю. Например, получить сведения об индексе `artem` в командной строке на spark-master-2.newprolab.com или spark-master-3.newprolab.com:

`curl http://artem.trunov:мой пароль@10.0.0.5:9200/artem`
где мой пароль это пароль от artem.trunov для https://lk-spark-de.newprolab.com

(далее в примерах аутентификация опущена)
curl -X PUT -H "Content-Type: application/json" 'http://artem.trunov:мой пароль@10.0.0.5:9200/artem_trunov_lab08?include_type_name=...


## Create index

```
curl -X PUT -H "Content-Type: application/json" 'http://10.0.0.5:9200/artem_trunov_lab08?include_type_name=false' -d @-<<END
{
   "settings" : {
        "number_of_shards" : 1,
        "number_of_replicas" : 1
    },

   "mappings": {
    "properties": {
      "uid": {
        "type": "keyword",
        "null_value": "NULL"
      },
      "gender_age": {
        "type": "keyword"
      },
      "date": {
        "type": "date"
      }
    }
  }
}
END
```

## Drop index

`curl -X DELETE http://10.0.0.5:9200/artem_trunov_lab08`

## Bulk load

Требуется создать файл с дополнительными json-сообщениями на каждую реальную запись. Но зато можно загрузить их эластиком одним запросом.

Если ваши сообщения выглядят вот так:

```
{"gender_age":"F:18-24","uid":"0000e7ca-32e6-4bef-bdca-e21c025071ff","date":1590969600000}
{"gender_age":"M:45-54","uid":"009acdaa-e72d-46c3-b1a5-1d8e06781594","date":1590969600000}
```

то файл для загрузки должен выглядеть вот так:

```
{"index": {"_index": "artem_trunov_lab08", "_type": "_doc", "_id": 0}}
{"gender_age":"F:18-24","uid":"0000e7ca-32e6-4bef-bdca-e21c025071ff","date":1590969600000}
{"index": {"_index": "artem_trunov_lab08", "_type": "_doc", "_id": 1}}
{"gender_age":"M:45-54","uid":"009acdaa-e72d-46c3-b1a5-1d8e06781594","date":1590969600000}
```
Обратите внимание на _id, который увеличивается.

Далее загрузка этого файла (здесь под названием data.json.elastic_bulk):

`curl -X POST -H "Content-Type: application/json" artem.trunov:мой пароль@10.0.0.5:9200/artem_trunov_lab08/_bulk --data-binary @data.json.elastic_bulk`
