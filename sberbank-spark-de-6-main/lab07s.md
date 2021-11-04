# Lab07s. Обучение и инференс с использованием sci-kit learn.

В Лаб07 вы определили пайплайн модели использую Spark ML, обучили ее и написали код для предсказания на новых данных. В этой работе будет принципиально все тоже самое, но только обучение модели будет происходить с помощью библиотеки sklearn на питоне. 

Дело тут вот в чем. SparkML - довольно ограниченная библиотека, по крайней мере с точки зрения дата-сайентиста. В ней нет многих алгоритмов, к которым привыкли ДС. Однако спарк обладает несомненным преимуществом в параллельной обработке большого количества данных. А так же разработчики спарка дали возможность расширять библиотеку собственными классами. Этим мы и воспользуемся.

В это лабораторной работе вы объедините оба мира, взяв от каждого лучшее.

## Задача с высоты птичьего полета.

Возьмите за основу ваш pipeline из Лаб07. 

Добавьте в этот пайплайн собственноручно написанный класс-трансформер для преобразования url в domain (опционально: и какого-нибудь из признаков, которые вы разрабатывали в Лаб06).

Напишите собственный класс-эстиматор. В этом эстиматоре в методе fit() вы будете вызывать программу для обучения модели на питоне с использованием sklearn, в которой подаются данные для обучения, а обученную модель вы сохраняете в переменной класса. В методе transform() вы тоже будете вызывать питоновский скрипт, применяющий модель sklearn для предсказаний.

Обученный пайплайн сохраняется на диск обычным образом, как и в Лаб07а. Далее вы применяете тот же самый код для инференса, как и в лаб07.

Этот метод может показаться спорным, ведь питоновская программа для обучение должна запускаться один раз на драйвере, и будет ограничена по памяти, то есть ей придется давать датасет небольшого размера. Но на практике это не так страшно - сэмплируют небольшой датасет из большого и это не приводит к существенной потере качества модели.

## Конкретика

## Оформление работы для решения со SparkML и Pipeline.

Поместите ваш проект в `lab07s/mlproject`. Файлы `train.scala` и `test.scala` переименуйте в `train_s.scala` и `test_s.scala` соответственно. Остальные требования - как и в Лаб07.

В лаб07 вы, вероятно, продублировали код для экстракции доменов и в программе тренировки и в программе для инференса. Когда вы напишите собственный трансформер для экстрации доменов, в этом не будет необходимости. Таким образом, можно определить модель и все спецефические для модели данные (такие как схемы для кафки) в отдельном файле, который можно импортировать в программы для обучения и инференса, которые можно применять для любых моделей.

### Модель

Добавьте в проект следующую иерархию папок: `org/apache/spark/ml/feature/`. В папке `feature` создайте файлы: `estimator.scala` и `transformer.scala`.

Внутри train_s.scala и test_s.scala используйте класс `Pipeline` и классы трансформера и эстиматора, которые определите (см. ниже).

#### Transformer

Здесь и далее приведены фрагменты ("скелет") кода, который поможет Вам разобраться в работе трансформеров и эстиматоров Spark'а. Вам не обязательно их использовать и Вы можете вносить свои коррективы в данную структуру. Также перед началом работы советуем ознакомиться как реализованы трансформеры и эстиматоры непосредственно в пакете Spark'а.

Класс трансформера назовите Url2DomainTransformer. Данный класс будет наследоваться от классов Transformer и DefaultParamsWritable (для возможности сохранения пайплайна на hdfs).

```scala
class Url2DomainTransformer(override val uid: String) extends Transformer
  with DefaultParamsWritable
{
  def this() = this(Identifiable.randomUID("org.apache.spark.ml.feature.Url2DomainTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    ... // Необходимые преобразования
  }

  override def copy(extra: ParamMap): Url2DomainTransformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
      ... // Определение выходной схемы данных
  }

}
```

Для возможности считывания пайплайна из hdfs в этом же файле определите объект `Url2DomainTransformer`, который наследуется от `DefaultParamsReadable[Url2DomainTransformer]`:

```scala
object Url2DomainTransformer extends DefaultParamsReadable[Url2DomainTransformer] {
  override def load(path: String): Url2DomainTransformer = super.load(path)
}
```

#### Estimator

Класс эстиматора назовите `SklearnEstimator` и унаследйте его от `Estimator[SklearnEstimatorModel]` и `DefaultParamsWritable`:

```scala
class SklearnEstimator(override val uid: String) extends Estimator[SklearnEstimatorModel]
  with DefaultParamsWritable
{
  def this() = this(Identifiable.randomUID("SklearnEstimator"))

  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {
      ...
      // Внутри данного метода необходимо вызывать обучение модели при помощи train.py. Используйте для этого rdd.pipe(). 
      // Файл train.py будет возвращать сериализованную модель в формате base64.
      // Данный метод fit возвращает SklearnEstimatorModel, поэтому инициализируйте данный объект, где в качестве параметра будет приниматься модель в формате base64.
  }

  override def copy(extra: ParamMap): SklearnEstimator = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // Определение выходной схемы данных
  }

}
```

Для возможности считывания пайплайна из hdfs в этом же файле определите объект `SklearnEstimator`, который наследуется от ` DefaultParamsReadable[SklearnEstimator]`:

```scala
object SklearnEstimator extends DefaultParamsReadable[SklearnEstimator] {
  override def load(path: String): SklearnEstimator = super.load(path)
}
```

Теперь необходимо создать класс `SklearnEstimatorModel`, объект которого возвращается в определенном выше методе fit. Данный класс отвечает за применение модели. Класс будет наследоваться от `Model[SklearnEstimatorModel]` и `MLWritable` (определите в этом же файле):

```scala
class SklearnEstimatorModel(override val uid: String, val model: String) extends Model[SklearnEstimatorModel]
 with MLWritable
{
  //как видно выше, для инициализации объекта данного класса в качестве одного из параметров конструктора является String-переменная model, это и есть модель в формате base64, которая была возвращена из train.py

  override def copy(extra: ParamMap): SklearnEstimatorModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    ...      
    // Внутри данного метода необходимо вызывать test.py для получения предсказаний. Используйте для этого rdd.pipe(). 
    // Внутри test.py используется обученная модель, которая хранится в переменной `model`. Поэтому перед вызовом rdd.pipe() необходимо записать данное значение в файл и добавить его в spark-сессию при помощи sparkSession.sparkContext.addFile.
    // Данный метод возвращает DataFrame, поэтому полученные предсказания необходимо корректно преобразовать в DF.
         
  }

  override def transformSchema(schema: StructType): StructType = {
    ...// Определение выходной схемы данных
  }

  override def write: MLWriter = new SklearnEstimatorModelWriter(this)
}
```

Для корректной работы модели с hdfs необходимо в этом же файле определить объект `SklearnEstimatorModel`, унаследованный от `MLReadable[SklearnEstimatorModel]`:
```scala
object SklearnEstimatorModel extends MLReadable[SklearnEstimatorModel] {
  private[SklearnEstimatorModel]
  class SklearnEstimatorModelWriter(instance: SklearnEstimatorModel) extends MLWriter {

    private case class Data(model: String)

    override protected def saveImpl(path: String): Unit = {
      // В данном методе сохраняется значение модели в формате base64 на hdfs
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.model)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SklearnEstimatorModelReader extends MLReader[SklearnEstimatorModel] {

    private val className = classOf[SklearnEstimatorModel].getName

    override def load(path: String): SklearnEstimatorModel = {
      // В данном методе считывается значение модели в формате base64 из hdfs
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("model")
        .head()
      val modelStr = data.getAs[String](0)
      val model = new SklearnEstimatorModel(metadata.uid, modelStr)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[SklearnEstimatorModel] = new SklearnEstimatorModelReader

  override def load(path: String): SklearnEstimatorModel = super.load(path)
}
```

### Скрипт для обучения модели на питоне

Воспользуйтесь LogisticRegression из пакета scikit-learn. Вот примерный, неотлаженный скрипт для обучения. Тут использова "классический" подход к обучению модели с датафреймами. Вы можете воспользоваться и массивами numpy вместо датафрейма. Вероятно, так будет даже проще.


```python
#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import sys

from sklearn.linear_model import LogisticRegression

rows = [] #here we keep input data to Dataframe constructor

# iterate over standard input
for line in sys.stdin:
  #parse line into a dict: {"column1": value1, ...}
  line_dict = ...
  rows.append(line_dict)

#initialize a dataframe from the list
df = pd.DataFrame(rows)

feature_columns = [...]
label_column = "gender_age"

model = LogisticRegression()
model.fit(df[feature_columns], df[label_column])
model_string = base64.b64encode(pickle.dumps(model)).decode('utf-8')

# Output to stdin, so that rdd.pipe() can return the string to pipedRdd.
print(model_string)
```

### Скрипт для инференса модели на питоне

```python
#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import sys

from sklearn.linear_model import LogisticRegression

#read the model, deserialize and unpickle it.

model = pickle.loads(
          base64.b64decode(
            open("lab07.model").read().encode('utf-8')
          )
        )

rows = [] #here we keep input data to Dataframe constructor

# iterate over standard input
for line in sys.stdin:
  #parse line into a dict: {"column1": value1, ...}
  line_dict = ...
  rows.append(line_dict)

#initialize a dataframe from the list
df = pd.DataFrame(rows)

#run inference
pred = model.predict(df)

# Output to stdin, so that rdd.pipe() can return the strings to pipedRdd.
print(pred)
```

### Подсказки

Выясните формат передачи данных в программу, вызываемую rdd.pipe() с помошью утилиты cat:

`training.rdd.pipe("/bin/cat").collect().foreach(println)`

Если программа на дает с ошибкой памяти (см. причины в описании задачи), семплируйте датасет перед подачей в пайплайн. Естественно, хорошо бы сделать стратифицированную выборку по классам.

Если проблемы с памятью на тестовом датасете, попробуйте делать предсказания "чанками".

Если у вас всё совсем плохо с памятью, воспользуйтесь опциями --driver-memory 4G, --executor-memory 4G, начиная с небольших занчений, пока не заработает.

Если возникают вопросы по оформлению трансформеров и эстиматоров - стоит обратить внимание на стандартные spark-классы и посмотреть как это реализовано в них.

Для возможности работы rdd.pipe() в стриминге необходимо использовать `.foreachBatch`.

Если падают скрипты обучения питона, то возможно в каких-то из фичей есть null, либо другие некорректные значения. Попробуйте для начала передавать 1-2 фичи для теста.

Если не создать иерархию папок `org/apache/spark/ml/feature/`, то чтение и запись на hdfs не будут работать корректно.

Если spark-сессия не видит файлы train.py и test.py (такое может возникнуть из-за выставленных прав на стороне кластера), то запустите сессию в режиме `client`.

### Тренировка 

Так же, как и в Лаб07.

### Предсказание

Так же, как и в Лаб07.

## Проверка

Так же, как и в Лаб07.

### Поля чекера

Те же, что и в Лаб07.
