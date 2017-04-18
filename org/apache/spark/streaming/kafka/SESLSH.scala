package org.apache.spark.streaming.kafka

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import kafka.serializer.StringDecoder
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkEnv, SparkConf, SparkContext}
import lsh.{L2, LSHModel, LSH}

/**
 * @author KevinZwx at 3/18/2017
 */
object SESLSH {
//    final val dataFile = "file:///srv/zwx/image_feature/image_feature.txt"
    final val dataFile = "file:///srv/zwx/image_feature/feature_100k.txt"
    final val modelVersion = "100k-RDD"
    final val non_replacement = false
    final val checkpoint_path = "file:///srv/spark/streaming/checkpoints"
    final val APPROXIMATE = true
    final val EXACT = false
    //  final val num9NN = 12247

    def main(args: Array[String]) {
        if (args.length != 2) {
            println("usage:<i for index or q for query> <number of hashTables for index or radius for query>")
            System.exit(1)
        }

        val conf = new SparkConf()
            .setAppName("SES-LSH")
        val sc = new SparkContext(conf)
        val modelPath = "file:///srv/zwx/lsh-models/model-" + modelVersion
        val data = loadDataInRDD(sc, dataFile, 256)
//        val data = loadData(sc, dataFile, 256)
        val dataRDD = data.cache()
        println(s"${
            dataRDD.count()
        } points loaded.")

        args(0) match {
            case "i" =>
                try {
                    index(sc, dataRDD, args(1).toInt, modelPath)
                }
                catch {
                    case nfe: NumberFormatException => println("number of hashTables should be Int.")
                    case e: Exception => e.printStackTrace()
                }
            case "q" =>
                try {
                    query(sc, dataRDD, args(1).toDouble, APPROXIMATE, modelPath)
                }
                catch {
                    case nfe: NumberFormatException => println("radius should be Double.")
                    case e: Exception => e.printStackTrace()
                }
                query(sc, dataRDD, args(1).toDouble, APPROXIMATE, modelPath)
            case "qe" =>
                try {
                    query(sc, dataRDD, args(1).toDouble, EXACT, modelPath)
                }
                catch {
                    case nfe: NumberFormatException => println("radius should be Double.")
                    case e: Exception => e.printStackTrace()
                }
            case _ =>
                println("usage:<i for index or q for query> <number of hashTables for index or radius for query>")
                System.exit(1)
        }
    }

    def index(sc: SparkContext, dataRDD: RDD[(Long, Vector)], numHashTables: Int, modelPath: String) = {
        val constructTime = System.currentTimeMillis()
        val dimensions = 1024
//        val numHashTables = 200
        val numFunctions = 4
        val binLength = 16

        val lshModel = LSH.train(dataRDD, dimensions, numFunctions, numHashTables, binLength)
        println(s"cost ${
            (System.currentTimeMillis() - constructTime) / 1000
        }s" +
            s" for loading data and table construction")

        lshModel.save(sc, modelPath)
    }

    def query(sc: SparkContext, dataRDD: RDD[(Long, Vector)], radius: Double, isApproximate: Boolean, modelPath: String) = {
        val ssc = new StreamingContext(sc, Seconds(1))
        //    ssc.checkpoint(checkpoint_path)
        val startTime = System.currentTimeMillis()
        val loadedModel = LSHModel.load(sc, modelPath)
        loadedModel.hashTables.cache()
        println(s"${
            loadedModel.hashTables.count()
        } points loaded.")
        println(s"cost ${
            (System.currentTimeMillis() - startTime) / 1000
        }s" +
            s" for loading data and table construction")

        val topics = Set("feature")
        println("-----------------------< " + String.format("%1$19s", "Waiting for Request") + " >-----------------------")
        // kafka
        val features = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (ssc, createKafkaParam, topics)
        // zeromq
        //    val features = ZeroMQUtils.createStream(ssc, "ipc:///home/zwx/feature.pipe", )
        features.foreachRDD {
            feature =>
                if (!feature.isEmpty()) {
                    println("-----------------------< " + String.format("%1$19s", "Request Received ") + " >-----------------------")
                    val featureVector = feature.first()
                    println(featureVector._1, featureVector._2)
                    if (featureVector._2 != null && !featureVector._2.isEmpty) {
                        //          featureVector._2.split("   ").filter(!_.trim.isEmpty).foreach(println)
                        val queryVector = Vectors.dense(featureVector._2.split("   ").filter(!_.trim.isEmpty).map(_.toDouble))
                        val queryDimension = queryVector.size
                        println(s"dimension of query vector: $queryDimension")
                        //    search for RNNs
                        val RNNTime = System.currentTimeMillis()
                        val RNNNeighbors =
                            if (isApproximate) LSH.RNNSearchV4 (loadedModel, dataRDD, queryVector, radius)
                            else exhaustedSearch(dataRDD, queryVector, radius)
                        //        val RNNNeighbors = exhaustedSearch(data, queryVector, radius)
                        println(s"RNN neighbors(${
                            RNNNeighbors.length
                        }):")
                        println(s"cost ${
                            System.currentTimeMillis() - RNNTime
                        }ms for RNN search")
                        //        RNNNeighbors.foreach(b => println(s"vid: ${b._1}, distance: ${b._2}"))
                        RNNNeighbors.take(5).foreach(b => println(s"neighbor' vid: $b"))
                        println("-----------------------< " + String.format("%1$19s", "Response Complete ") + " >-----------------------")
                        //    println(s"recall: ${RNNNeighbors.length.toDouble / num9NN}%")
                        //        println(s"cost ${(System.currentTimeMillis() - startTime) / 1000}s in total")
                    }
                }
            //      println("no feature extracted.")
        }
        ssc.start()
        ssc.awaitTermination()
    }

    def createKafkaParam(): Map[String, String] = {
        val brokers = "10.107.8.30:9092,10.107.20.24:9092,10.107.19.21:9092,10.107.19.16:9092"
        Map[String, String] (
            "metadata.broker.list" -> brokers,
            "serializer.class" -> "kafka.serializer.StringEncoder",
            "auto.offset.reset" -> "largest")

    }

    def exhaustedSearch(data: RDD[(Long, Vector)], query: Vector, radius: Double): Array[Long] = {
        data.map(v => (v._1, L2(v._2, query))).filter(_._2 <= radius).map(_._1).collect()
    }

    def loadData(sc: SparkContext, file: String, numPartitions: Int): IndexedRDD[Long, Vector] = {
        val data = sc.textFile(file, numPartitions).zipWithIndex().map(x => {
            val array = x._1.split("   ")
            (x._2, Vectors.dense(array.map(_.toDouble)))
        })
        IndexedRDD(data)
    }

    def loadDataInRDD(sc: SparkContext, file: String, numPartitions: Int): RDD[(Long, Vector)] = {
        val data = sc.textFile(file, numPartitions).zipWithIndex().map(x => {
            val array = x._1.split("   ")
            (x._2, Vectors.dense(array.map(_.toDouble)))
        })
//        if (data.partitioner.isDefined) data
//        else
//            data.partitionBy(new HashPartitioner(numPartitions))
        data.repartition(numPartitions)
    }
}
