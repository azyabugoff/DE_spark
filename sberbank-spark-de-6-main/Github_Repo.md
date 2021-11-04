# Настройка и работа с Git-репозиторием

Создайте на github.com _приватный_ репозиторий под названием `sb-spark`. При создании выберите опцию создать .gitignore для scala. Так же добавьте *.jar и .idea в .gitignore. И не добавляете jar-файлы в репозиторий.

В этот репозиторий вы будете заливать свои решения, начиная с Лабы 3, и наличие решений будет проверяться чекером.

Добавьте в этот репозиторий публичный `deploy key`, используя инструкцию с [сайта GitHub](https://developer.github.com/v3/guides/managing-deploy-keys/#deploy-keys).

### Откуда взять ключ

У вас в ЛК в разделе [Персональная информация](https://lk-spark-de.newprolab.com/access) имеется сгенерированный для вас приватный ключ.

Загрузите его кнопкой "Загрузить" или путем копирования и вставки в пустой файл. Вы можете сделать это не кластере. Если у вас Мас или Линукс, то можете делать это на нем.

Далее, сгенерируйте публичную часть следующей командой (предполагается, что ваш приватный ключ хранится в файле npl.pem):

```
$ ssh-keygen -y -f npl.pem
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCq0/maeQOdgbP24eajdfarrdpqWm
FXy2dUoPImZGCYlz8eGmekhidYr2+9U4dWx97LRv5Vrckf0p/PHFwdsioDh5kweAy6L2vSN4qJ8nf6mwJ9OnWlV+C6
...
```

Выделите, скопируйте и вставьте в соответствующее поле на гитхабе.

### Самопроверка

```
chmod 600 npl.pem
export GIT_SSH_COMMAND="ssh -i npl.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
git clone git@github.com:$user/sb-spark.git
```
Где $user - имя вашего аккаунта на GitHub.
Вы не должны получить permission denied.

## Оформление работ в репозитории

Для каждой лабораторной работы вы будете создавать папку с названием работы, например `lab03`. В ней создается подпапка для проекта, например с названием `data_mart`. В ней - sbt-project под названием `data_mart` с главным классом `data_mart` в файле `data_mart.scala`. Этот файл может размещаться прямо в папке `data_mart` и в `data_mart/src/main/scala`.

Проект должен компилироваться и запускаться следующим образом:

```
cd lab03/data_mart
sbt package
spark-submit --class data_mart .target/scala-2.11/data_mart_2.11-1.0.jar 
```

В некоторых работах мы будем собирать и запускать ваш проект. Пожалуйста не добавляйте jar-файлы в репозиторий. Чекер будет проверять наличие *.jar в .gitignore и останавливаться с ошибкой, если такого правила нет.
