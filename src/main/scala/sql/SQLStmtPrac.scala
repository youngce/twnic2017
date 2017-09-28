package sql

import org.apache.spark.sql.SparkSession

object SQLStmtPrac extends App{
  val ss=SparkSession.builder()
    .master("local[*]")
    .appName("ml-20m")
    .getOrCreate()
  val reader=ss.read.option("header", "true").option("inferSchema", "true")
  reader.csv("src/main/resources/sql/ml-20m/ratings.csv").createOrReplaceTempView("ratings")
  reader.csv("src/main/resources/sql/ml-20m/movies.csv").createOrReplaceTempView("movies")

  /**Examples*/
  /**計算各movieId的平均rating*/
  ss.sql(
    """SELECT  movieId,avg(rating), variance(rating) as var1  FROM ratings
      |where isnan(variance(rating))=false
      |GROUP BY movieId""".stripMargin).show()

//  /**計算各movieId的平均rating，並找出對應的movie title*/
//  ss.sql("""SELECT m.movieId as mid, first(title) as title, avg(rating) as average FROM ratings as r JOIN movies as m
//           |WHERE r.movieId==m.movieId
//           |GROUP BY m.movieId""".stripMargin).show()
//
//
//  /**計算各movieId的平均rating，並找出對應的movie title，最後找出最高的前10部電影*/
//  ss.sql(
//    """
//      |SELECT m.movieId as mid, title, avg(rating) as average FROM ratings as r JOIN movies as m
//      |WHERE r.movieId==m.movieId
//      |GROUP BY mid,m.title
//      |ORDER BY average DESC
//      |LIMIT 10
//    """.stripMargin).show()

}
