package task1_books

import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import provider.{DefaultSparkSessionProvider, SparkSessionProviderComponent}


object task1_booksDatasetApp extends App
  with SparkSessionProviderComponent
  with task1_booksDataset {
    override def sparkSessionProvider = new DefaultSparkSessionProvider("task1_booksDatasetApp")
    books("books.csv")
}

trait task1_booksDataset {
  this: SparkSessionProviderComponent =>

  private val sparkSession = sparkSessionProvider.sparkSession

  import sparkSession.implicits._

  def books(path: String): Unit = {

    val textDF: DataFrame = sparkSession.read
      .option("header", "true")
      .csv(path)
      .withColumn("average_rating", $"average_rating".cast(sql.types.DoubleType))
      .na.drop()

    textDF.printSchema()

    println("count = " + textDF.count())

    textDF.filter(textDF("average_rating") > 4.5).show(Int.MaxValue)

    textDF.agg(avg("average_rating")).show()

    textDF.select($"average_rating", $"title",
      when($"average_rating" >= 0 && $"average_rating" <= 1, ("0 - 1"))
        .when($"average_rating" > 1 && $"average_rating" <= 2, ("1 - 2"))
        .when($"average_rating" > 2 && $"average_rating" <= 3, ("2 - 3"))
        .when($"average_rating" > 3 && $"average_rating" <= 4, ("3 - 4"))
        .when($"average_rating" > 4 && $"average_rating" <= 5, ("4 - 5")) as "average_rating_interval")
      .groupBy("average_rating_interval")
      .count()
      .show()
  }
}
