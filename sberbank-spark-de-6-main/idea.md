# Настройка проекта в IDEA

Для выполнения лабораторных работ можно использовать любую удобную среду разработки, в качестве базового решения предлагается использоваться среду разработки Intellij IDEA.

## 1. Скачивание и установка

IDEA Community доступна для бесплатного скачивания на сайте JetBrains:

https://www.jetbrains.com/ru-ru/idea/download

![Alt text](images/idea1.png?raw=true "Cтраница загрузки")

После скачивания производим установку (все опции можно оставить по умолчанию):

![Alt text](images/idea2.png?raw=true "Установка 1")

![Alt text](images/idea3.png?raw=true "Установка 2")

![Alt text](images/idea4.png?raw=true "Установка 3")

В данном окне необходимо дополнительно выбрать установку scala-плагина:

![Alt text](images/idea5.png?raw=true "Установка 3")


## 2. Создание проекта

После установки создадим новый scala-проект. Для этого выберем "Create New Project":

![Alt text](images/idea6.png?raw=true "Создание проекта 1")
 
В открывшемся окне выберем "Scala" и "sbt":
 
![Alt text](images/idea7.png?raw=true "Создание проекта 2")
 
Задаем имя проекта, выбираем папку для сохранения. В качестве JDK выбираем версию 1.8. В качестве версии scala 2.11.12. В качестве версии sbt 1.3.13.

![Alt text](images/idea8.png?raw=true "Создание проекта 3")
 
Если у вас не установлен JDK, то его также необходимо скачать и установить. JDK доступен на сайте Oracle по следующей ссылке:
 
https://www.oracle.com/ru/java/technologies/javase-downloads.html
 
Необходимо выбрать "JDK Download" для 8 версии:
 
![Alt text](images/idea9.png?raw=true "Скачивание JDK")

После создания проекта у нас отроется следующее окно:

![Alt text](images/idea10.png?raw=true "Проект 1")

Для проверки корректности работы добавим файл temp.scala в директорию scala со следующим кодом:

```scala
object Hello {
    def main(args: Array[String]) = {
        println("Hello, world")
    }
}
```

После этого IDEA может вывести сообщение "No Scala SDK in module"":

![Alt text](images/idea11.png?raw=true "Проект 2")

Нажимаем "Setup Scala SDK" и затем "Create:

![Alt text](images/idea12.png?raw=true "Настройка SDK 1")

Выбираем версию Scala 2.11.12:

![Alt text](images/idea13.png?raw=true "Настройка SDK 2")


![Alt text](images/idea14.png?raw=true "Настройка SDK 2")

Жмем "OK" и после этого scala-синтаксис должен корректно определяться:

![Alt text](images/idea15.png?raw=true "Проект 3")

Для запуска проекта можно нажать правую кнопку мыши на имени объекта "Hello" и выбрать "Run 'Hello'":

![Alt text](images/idea16.png?raw=true "Проект 4")

После этого внизу в консоли появится вывод результатов работы программы, а слева вверху вместо "Add Configuration..." появится имя запускаемой задачи и в дальнейшем проект можно запускать нажатием на зеленую кнопку старта:

![Alt text](images/idea17.png?raw=true "Проект 5")

Если нажать на кнопку с жучком, то запустится debug-режим с возможностью работы с точками останова, которые ставятся при клике на номер строки в редакторе кода:

![Alt text](images/idea18.png?raw=true "Проект 6")

Для сборки jar-файла необходимо справа нажать на вкладку "sbt", выбрать "sbt-tasks" и дважды кликнуть на package:

![Alt text](images/idea19.png?raw=true "Проект 7")

После этого jar-файл появится в папке target/scala-2.11/:

![Alt text](images/idea20.png?raw=true "Проект 8")
