import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import util.control.Breaks._
import scala.collection.Map
import scala.collection.immutable.ListMap
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ddsPhase3 {

	def main(args: Array[String]) {
		/*Initialize parameters*/
		val conf = new SparkConf().setAppName("Getis-Ord Statistic Calculation").setMaster("local")
		val sc = new SparkContext(conf)
		val sqlContext= new org.apache.spark.sql.SQLContext(sc)
		import sqlContext.implicits._
		val spark = SparkSession.builder().getOrCreate()

		val inputPath = args(args.length-2)
		val outputPath = args(args.length-1)
		
		/*Identify boundary of the region*/
	    val MINLAT:scala.Double = 40.50;
	    val MAXLAT:scala.Double = 40.90;
	    val MINLONG:scala.Double = -74.25;
	    val MAXLONG:scala.Double = -73.70;
	  
		val NUMBEROFCELLS:scala.Double = 68200.0;//55 * 40 * 31
		val NUMBEROFHOTSPOTS:scala.Int = 50;
		val CELLUNITSIZE:scala.Double = 0.01;

		/*Read the input file and typecast the columns of the DataFrame*/
		val inputDataFrame = spark.read.format("csv").option("header", "true").load(inputPath)
		.select("tpep_pickup_datetime", "pickup_longitude", "pickup_latitude")
		.withColumn("pickup_longitude",col("pickup_longitude").substr(0, 6).cast("double"))
		.withColumn("pickup_latitude",col("pickup_latitude").substr(0, 5).cast("double"))
		.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").substr(9, 2).cast("int")-1)
		.filter($"pickup_longitude" >= MINLONG && $"pickup_longitude" <= MAXLONG
				&& $"pickup_latitude" >= MINLAT && $"pickup_latitude" <= MAXLAT)
				
		inputDataFrame.registerTempTable("taxi")

	    val groupedDataFrame = spark.sql("select pickup_longitude,pickup_latitude,tpep_pickup_datetime, count(*) as "+
		    "count, count(*)*count(*) as count2 from taxi group by tpep_pickup_datetime,pickup_longitude,pickup_latitude")
		    
	    groupedDataFrame.cache()
  	
	    val TOTAL_POINTS:scala.Double = groupedDataFrame.agg(sum("count")).first().getLong(0)
	    val SUMOFSQUARES:scala.Long = groupedDataFrame.agg(sum("count2")).first().getLong(0)
	    val MEAN:scala.Double = TOTAL_POINTS/NUMBEROFCELLS
		val STD_DEV:scala.Double = math.sqrt((SUMOFSQUARES/NUMBEROFCELLS) - MEAN*MEAN)
    
	    var groupedDataRDD = groupedDataFrame.rdd
        .map(x => (((x.get(0).asInstanceOf[scala.Double], x.get(1).asInstanceOf[scala.Double], 
        x.get(2).asInstanceOf[Int]), (x.get(3).asInstanceOf[scala.Long], x.get(4).asInstanceOf[scala.Long]))))

        var groupedDataMap = groupedDataRDD.collectAsMap()
    	val groupedDataMapBroadcast = sc.broadcast(groupedDataMap)
    
	    /*Function to calculate the wijxj and wij values*/
	    def calculatewx(x:scala.Double, y:scala.Double, date:scala.Int) : (scala.Long, scala.Int) = {
			val broadcast = groupedDataMapBroadcast.value
			var time:scala.Int = 0
			var wij:scala.Int = 0
			var wij_xj:scala.Long = 0L
			for(time:scala.Int <- List(date-1,date,date+1)){
				breakable{
					if( time > 30 || time < 0)
						break
					for(i:scala.Double <- List(x-CELLUNITSIZE,x,x+CELLUNITSIZE)){
						breakable{
							val a:scala.Double = (math round(i *100 + 1)) /100.0
							if(i > MAXLONG || i < MINLONG)
		  						break
							for(j:scala.Double <- List(y-CELLUNITSIZE,y,y+CELLUNITSIZE)){
								breakable{
									val b:scala.Double = (math round(j*100)) /100.0
									if(j > MAXLAT || j < MINLAT)
										break
									wij+=1
									wij_xj+= broadcast.get((a,b,time)).map(_._1.toLong).getOrElse(0L)
								}
							}
						}
					}
				}
			}
			(wij_xj, wij)
    	}

		val finalValuesRDD = groupedDataRDD.map(x=> (x._1,calculatewx(x._1._1,x._1._2,x._1._3)))
    	var finalValuesMap:Map[(scala.Double, scala.Double, Int), scala.Double] = 
      	finalValuesRDD.map(x => ((x._1) ,((x._2._1 - MEAN*x._2._2)/
        (STD_DEV*math.sqrt((NUMBEROFCELLS*x._2._2-(x._2._2*x._2._2))/(NUMBEROFCELLS-1))))))
        .collectAsMap()
		val sortedValues = ListMap(finalValuesMap.toSeq.sortBy(-_._2):_*).take(NUMBEROFHOTSPOTS)
		sc.makeRDD(sortedValues.map{f => var line : String = f._1._2+","+f._1._1+","+f._1._3+","+f._2;line}.toList)
		.saveAsTextFile(outputPath)
	}
}