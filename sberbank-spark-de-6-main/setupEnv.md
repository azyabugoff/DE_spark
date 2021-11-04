
# Hardware

В рамках прохождение обучение возможно использование: личного или корпоративного ноутбука.

В случае наличия обоих вариантов рекомендуется использовать свой личный ноутбук.\
Основной аргумент: гибкость настройки ОС/ПО в рамках имеющихся ограничений по безопасности (отсутствие админских прав). Т.е. личный ноут предоставляет возможности "песочницы", где все можно донастроить и до-/переставить и проще решить возникшие проблемы.\
Также использование личного ноутбука особенно важно, если у него более сильные характеристики (современные IDE довольно прожорливые).

# Software

Требуется установить IDEA+JDK:
* IDEA (community edition). При наличие лицензии рекомендуется ultimate edition.\
    * [оф-сайт для скачивания дистрибутива](https://www.jetbrains.com/idea/)
    * установить plugin for Scala (доступно в визарде при первичной настройке среды).
* JDK 1.8.0, last version: [download page for HotSpot](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html)

На копроративный ноутбук ПО может быть установлено через выставление заявок на установку в [sberusersoft](https://sberusersoft). 
В данный момент там доступны следующие пакеты:
* IDEA community/ultimate edition 2020.1,
* Java JDK 8 update Last x64.\
Также отдельно можно установить: Scala + SBT.

Установка данного ПО доступна только через согласование со стороны ИБ. Согласование происходит фоново для пользователя, но для его инициации необходимо указать причину (в нашем случае - это прохождение курсов повышения квалификации).\
Для удаленного подключения можно заказать следующее ПО:
* Putty (рекомендуется mobaxterm),
* WinSCP

**Важно**: через корпоративный wifi подключение по ssh, скорей всего, будет не доступно. Обходное - подключение с личной точки (с мобильного, либо с иного wifi)

## Ссылки

* IDE:
    * [Discover Intellij IDEA for Scala](https://www.jetbrains.com/help/idea/discover-intellij-idea-for-scala.html)
    * [Cloudera Tutorial](https://www.cloudera.com/tutorials/setting-up-a-spark-development-environment-with-scala/.html)
* Spark 2.4.5 [API docs: scala](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package) \
    поддержка scala 2.12 появилась в spark 2.4.0 как экспериментальная, но уже, начиная со spark 2.4.1, scala 2.11 является deprecated.\
    в spark 3.0 - scala 2.11 поддерживаться не будет.
* инструменты сборки:
    * SBT -  наиболее популярный для scala [sbt by example](https://www.scala-sbt.org/1.x/docs/sbt-by-example.html) / [download](https://www.scala-sbt.org/download.html)
    * Maven - старый добрый [scala with maven](https://docs.scala-lang.org/tutorials/scala-with-maven.html)
* Ammonite - interactive REPL [оф-сайт](http://ammonite.io/#Ammonite-REPL),
* Scala 2.11/2.12 [оф-сайт об установке и альтернативах](https://www.scala-lang.org/download/).
