package sql

import org.apache.spark.sql.SparkSession

object DataSetDemo extends App{

  case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
  case class Department(id: Int, name: String)

  case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
  case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

  val ss=SparkSession.builder()
    .master("local[*]")
    .appName("DataSet")
    .getOrCreate()
  import ss.implicits._
//  val sc=ss.sparkContext
//  val employeeDataSet1 = sc.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
//  val employeeDataSet2 = sc.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
//  val departmentDataSet = sc.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()
//
//  val employeeDataset = employeeDataSet1.union(employeeDataSet2)
//
//
//
//  val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
//        .map(_._1.name).withColumnRenamed("value","test")
//
//  averageSalaryDataset
//  averageSalaryDataset.show()
//
//  case class Person(name:String,age:Option[Int])
//
//  val people=ss.read.json("src/main/resources/sql/people.json").as[Person]
//
//  people.groupByKey(v=>{
//    v.age match {
//      case Some(i) if i>20=>"adult"
//      case None=>"unknown"
//      case _=> "kid"
//
//    }
//  }).count().show()

}
