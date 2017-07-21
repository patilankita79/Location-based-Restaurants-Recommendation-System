
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

var businessData = sc.textFile("ProjectDataset/yelp_academic_dataset_business_out.csv”)
var result_businessdata = businessData.map(line => line.split(",")).map(line => (line(14),(line(0),line(1),line(19),line(22),line(35),line(37),line(43),line(65),line(68)))).foreach(println)

var tipData = sc.textFile("ProjectDataset/yelp_academic_dataset_tip.csv")
var result_tipdata = tipData.map(line => line.split(",")).map(line => (line(2),line(1)))
var joinData = result_businessdata.join(result_tipdata).distinct().collect

var joinData = result_businessdata.join(result_tipdata).distinct().collect

var res1 = joinData.map{ case(str1,((str2,str3,str4,str5,str6,str7,str8,str9,str10),str21)) => s""""$str1"""" + "," +  s""""$str2"""" + "," +  s""""$str3"""" + "," +  s""""$str4"""" + "," +  s""""$str5"""" + "," +  s""""$str6"""" + "," +  s""""$str7"""" + "," +  s""""$str8"""" + "," +  s""""$str9"""" + "," +  s""""$str10"""" + "," +  s""""$str21""""}
var output_rdd = sc.parallelize(res1)
var myDf = output_rdd.coalesce(1,true)
myDf.saveAsTextFile("ProjectDataset/output_final")


