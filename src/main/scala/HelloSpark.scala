import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


/**
  * Created by mark on 13/09/2017.
  */
object HelloSpark extends App{
  val conf=new SparkConf().setMaster("local[2,2]").setAppName("hello")
  val sc=new SparkContext(conf)

  var failureTimes=0
  val res=sc.parallelize(1 to 100).map(v=>{
    if (v>99&&failureTimes<1){
      failureTimes+=1
      throw new Exception("Test Failure")
    }
    v
  }).sum()
  println(res)
  readLine()
}
