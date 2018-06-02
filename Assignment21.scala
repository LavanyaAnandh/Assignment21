import org.apache.spark.sql.{Column, SparkSession, UDFRegistration}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.col


object Assignment21 {
  case class Sports_cls(fname:String, lname:String, Sports:String, MedalType:String, Age:Long, Year:Long, country:String )


  def main(args: Array[String]): Unit = {

    //Creating spark Session object
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Assignment 21")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    println("Spark Session Object created")

  val Sports_data = spark.read.textFile("D:\\Lavanya\\Sports_data.txt")
    //Removing the header line from the building.csv file
    val header1 = Sports_data.first()
    val data3 = Sports_data.filter(row => row != header1)
    import spark.implicits._
    val data4 = data3.map(x=>x.toString()split(",")).map(x => Sports_cls(x(0),x(1),x(2),x(3),x(4).toLong,x(5).toLong,x(6))).toDF()

    data4.registerTempTable("MedalsList") //Registering as temporary table IPatientCharges.
    println("Dataframe Registered as table !")
   // data4.show()

    //To get the number of Gold Medal winners every year.
    val GoldMedals = spark.sql("select year, MedalType, count(MedalType) from MedalsList group by year, MedalType having MedalType = 'gold'")
    //GoldMedals.show()

    //To get the number of Gold Medal winners every year.
    val SilverMedals = spark.sql("select Sports, country, MedalType, count(MedalType) from MedalsList group by Sports, country, MedalType having MedalType = 'silver' and country = 'USA'")
    //SilverMedals.show()

    //udf to add a new column containing MR.+ 2charactersof first name + last name
   def changefname = org.apache.spark.sql.functions.udf((fname: String, lname: String) => {
     val newsubstrwith2char = fname.substring(0,2)
     var newsubstrMr: String = "Mr."
     var newsubstr1: String = ""
     newsubstr1 = newsubstrMr + newsubstrwith2char + " " + lname
     newsubstr1
   })
    val newname = data4.withColumn("NewName", changefname($"fname",$"lname"))
    newname.show(10)
    val droppedfield = newname.drop("fname","lname")
    val finaloutput = droppedfield.select("NewName","Sports","MedalType","Age","Year","country")
    finaloutput.show(20)

    //udf to Add a new column called ranking using udfs on dataframe
    def NewRankColumn = org.apache.spark.sql.functions.udf((medaltype: String, age: Long) => {
      var newsubstr:String = ""
      if(medaltype == "gold" && age >= 32)
        newsubstr +=  "PROF GOLD MEDALIST"
      if(medaltype == "gold" && age <= 31)
        newsubstr += "AMATEUR SILVER MEDALIST"
      if(medaltype == "silver" && age >= 32)
        newsubstr += "EXPERT SILVER MEDALIST"
      if(medaltype == "silver" && age <= 32)
        newsubstr += "Rookie"
      newsubstr
    })

    val newRank = data4.withColumn("Ranking", NewRankColumn($"MedalType",$"Age"))
    newRank.show(15)
  }
}

