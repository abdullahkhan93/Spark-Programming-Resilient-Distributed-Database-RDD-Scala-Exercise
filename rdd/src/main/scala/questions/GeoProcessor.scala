package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.math._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph


/** GeoProcessor provides functionalites to 
* process country/city/location data.
* We are using data from http://download.geonames.org/export/dump/
* which is licensed under creative commons 3.0 http://creativecommons.org/licenses/by/3.0/
*
* @param spark reference to SparkSession 
* @param filePath path to file that should be modified
*/
class GeoProcessor(spark: SparkSession, filePath:String) {

    //read the file and create an RDD
    //DO NOT EDIT
    val file = spark.sparkContext.textFile(filePath)

    /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array for each location
    */
    def filterData(data: RDD[String]): RDD[Array[String]] = {
        /* hint: you can first split each line into an array.
        * Columns are separated by tab ('\t') character. 
        * Finally you should take the appropriate fields.
        * Function zipWithIndex might be useful.
        */
	val fieldSelect = List(1, 8, 16)
	val splittedDataset = data.map(x => x.split('\t').zipWithIndex.filter { case (field, index) => fieldSelect.contains(index)}.map {_._1} ).collect()
	val filteredDatasetRDD = spark.sparkContext.parallelize(splittedDataset)
	filteredDatasetRDD
    }


    /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
    def filterElevation(countryCode: String,data: RDD[Array[String]]): RDD[Int] = {
	val filteredDataset = data.zipWithIndex.filter{ case (field, index) => field.contains(countryCode)}.map {_._1}
	val demDataset = filteredDataset.map(x => x.mkString("\t").split("\t")(2).toInt).collect()
	val demDatasetRDD = spark.sparkContext.parallelize(demDataset)
	demDatasetRDD
    }



    /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data: RDD containing only elevation information
    * @return The average elevation
    */
    def elevationAverage(data: RDD[Int]): Double = {
        val demDataset = data.zipWithIndex
	val demSum = (demDataset.map(x => x._1).sum()).toDouble
	val demCount = (demDataset.collect().length).toDouble
	val averageDem = demSum / demCount
	averageDem 
    }

    /** mostCommonWords calculates what is the most common 
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '. 
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of 
    * occurrences. RDD should be in descending order (sorted by number of occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
    def mostCommonWords(data: RDD[Array[String]]): RDD[(String, Int)] = {
        val splittedDataset = data.map(x => x.zipWithIndex.filter { case (field, index) => index == 0}.map {_._1}.mkString(""))
	val wordCount = splittedDataset.flatMap(line => line.split(' ')).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, false).collect()
	val wordcountRDD = spark.sparkContext.parallelize(wordCount)
	wordcountRDD
    }

    /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string "" if countrycodes.csv
    *         doesn't have that entry.
    */
    def mostCommonCountry(data: RDD[Array[String]], path: String): String = {
        val splittedDataset = data.map(x => x.zipWithIndex.filter { case (field, index) => index == 1}.map {_._1}.mkString(""))
	val countryCodeCount = splittedDataset.map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, false).zipWithIndex
	val countryList = spark.sparkContext.textFile(path)
	val freqCode = countryCodeCount.map{case ((v1, v2), k) => (k, (v1, v2))}.lookup(0).map{_._1}.mkString("")
	val frequentCountryList = countryList.zipWithIndex.filter { case (field, index) => field.contains(freqCode)}.map {_._1}
	val frequentCountry = frequentCountryList.collect().mkString(",").split(",")(0)
	frequentCountry
    }

//
    /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    *   if you want to use helper functions, use variables as
    *   functions, e.g
    *   val distance = (a: Double) => {...}
    *
    * @param lat latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
    def hotelsInArea(lat: Double, long: Double): Int = {
	var hotelCounter = 0    
	val earthRadius = 6371.0    
	val fieldSelect = List(1, 4, 5)
	val data = spark.sparkContext.textFile(filePath)
	val filteredDataset = data.map(x => x.split('\t').zipWithIndex.filter { case (field, index) => fieldSelect.contains(index)}.map {_._1}.mkString("\t"))
	val uniqueDataset = filteredDataset.distinct
	val splittedDataset = uniqueDataset.map(x => x.split('\t')).collect()
	for(record <- splittedDataset){
		if(record(0).toLowerCase.contains("hotel")){

			val dLat=(record(1).toDouble - lat).toRadians
      			val dLon=(record(2).toDouble - long).toRadians
 
      			val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat.toRadians) * cos(record(1).toDouble.toRadians)
      			val c = 2 * asin(sqrt(a))
      			if((earthRadius * c) <= 10.0){
				hotelCounter += 1
			}
		}
	}
	hotelCounter
    }

    //GraphX exercises

    /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    *         || ||
    *         || ||
    *         \   /
    *          \ /
    *           +
    *
    *         _ 3 _
    *         /' '\
    *        (1)  (1)
    *        /      \
    *       1--(2)--->2
    *        \       /
    *         \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/latest/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *  from the resources folder
    * @return graphx graph
    *
    */
    def loadSocial(path: String): Graph[Int, Int] = {
        val data = spark.sparkContext.textFile(path).filter{ case x => x.contains("|") && !(x.contains("_")) }
        val regex = "([ /t]{2,})*[A-Za-z_|+-]+([ /t]{2,})*"
	val splittedDataset = data.map(x => x.replaceAll(regex, "").trim.split(" ").zipWithIndex.filter { case (field, index) => field.matches("[\\d]+") }.map {_._1})
	var connectorDataset = splittedDataset.map(x => Array(x(0).trim.toLong, x(1).trim.toLong)).collect().distinct
        var nodeDataset = connectorDataset.flatten.distinct
        var nodes = nodeDataset.map(x => (x.toLong, x.toInt))
        var connectors = connectorDataset.map(x => Edge(x(0).toLong, x(1).toLong, connectorDataset.count(y => (y(0) == x(0) && y(1) == x(1))).toInt)).distinct
        val graph = Graph(spark.sparkContext.parallelize(nodes), spark.sparkContext.parallelize(connectors))
        graph
    }

    /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
    def mostActiveUser(graph: Graph[Int,Int]): Int = {
        val activeNode = graph.outDegrees.join(graph.vertices).sortBy(_._2._1, ascending = false).take(1)
        val activeUser = (activeNode(0)._1).toInt
	activeUser
    }

    /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
    def pageRankHighest(graph: Graph[Int,Int]): Int = {
        val levels = graph.pageRank(0.0001).vertices
        val detailedLevels = levels.join(graph.vertices).sortBy(_._2._1, ascending = false).map(_._2._2)
        val highRank = detailedLevels.take(1)
        val highestRankedUser = highRank(0).toInt  
	highestRankedUser      
    }
}
/**
*
*  Change the student id
*/
object GeoProcessor {
    val studentId = "662011"
}
