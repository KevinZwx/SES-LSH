package lsh

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD

/** Runner program. Returns a LSHModel of input data with given parameters.
  *
  * @param data  RDD of input data with format of (vectorId, SparseVector)
  * @param dimensions  dimensions of input data
  * @param numHashFunctions number of hash functions
  * @param numHashTables number of hash tables
  */
class LSH(data: RDD[(Long, Vector)], dimensions: Int, numHashFunctions: Int = 4, numHashTables: Int = 10, binLength: Double = 4.0) extends Serializable {

  def run(): LSHModel = {
    val model = new LSHModel(numHashFunctions, numHashTables, dimensions, binLength)
//    val dataRDD = data.cache()
    val combined = true

    // SES-LSH 3 and 4
//    model.hashTables = IndexedRDD(data
//      .map(v => (model.hashValue(v._2), v._1))
//      .flatMap(x => x._1.map(a => (a, x._2))), combined
//    ).cache()

    // SLSH
    model.hashTables = data
        .map(v => (model.hashValue(v._2), v._1))
        .flatMap(x => x._1.map(a => (a, x._2)))
        .groupByKey()
        .mapValues(_.toList).cache()

//    println(s"${model.hashTables.partitions.length} partitions in hashTableRDD")
    println(s"${model.hashTables.count()} hashTables construct")
//    println(model.hashTables.flatMap(_._2).distinct().count())
    model
  }
}

object LSH extends Serializable with Logging {

  def train(input: RDD[(Long, Vector)], dimensions: Int, numHashFunctions: Int = 4, numHashTables: Int = 10, binLength: Double = 4.0): LSHModel = {
    new LSH(input, dimensions, numHashFunctions, numHashTables, binLength).run()
  }

  /** searches kNN neighbors and returns their vIds and distances from v */
  def kNNSearch(model: LSHModel, data: RDD[(Long, Vector)], v: Vector, k: Int = 10): Array[(Long, Double)] = {
    val startTime = System.currentTimeMillis()
    val candidates = model.getCandidates0(v)
    //    val candidatesVectors = candidatesRDD.map(key => key->L2(v, data.lookup(key)(0))).take(k)
    val candidatesVectors = data.filter(d => candidates contains d._1)

    println(s"cost ${(System.currentTimeMillis() - startTime) / 1000}s to get candidates vectors")
    println(s"$candidatesVectors")

    candidatesVectors.map(c => (c._1, L2(v, c._2))).sortBy(_._2).take(k)
    //    candidatesVectors
  }

  /** searches RNN neighbors and returns their vIds and distances from v */
//  def RNNSearch(model: LSHModel, data: IndexedRDD[Long, Vector], v: Vector, r: Double = 0.03): Array[(Long, Double)] = {
//    //    val model = buildModel()
//    //val model = LSHModel.load(path)
//    //    val candidatesRDD = model.getCandidates(v)
//    //    println(candidatesRDD.count())
//
//    val startTime = System.currentTimeMillis()
//    val candidateIds = model.getCandidates0(v)
//    println(s"${candidateIds.length} candidates found")
//    val t1 = System.currentTimeMillis()
//    println(s"cost ${t1 - startTime}ms to get candidate ids")
//
//    /** duplicate elements will be skipped here */
//    //    val candidatesVectors = data.filter(d => candidates contains d._1)
//    val candidateVectors = data.multiget(candidateIds)
//    val t2 = System.currentTimeMillis()
//    println(s"cost ${t2 - t1}ms to look up vectors")
//    candidateVectors.map(x => (x._1, L2(v, x._2))).filter(x => x._2 < r).toArray
//  }

  def RNNSearchV4(model: LSHModel, data: RDD[(Long, Vector)], v: Vector, r: Double = 20): Array[Long] = {
    val startTime = System.currentTimeMillis()
    val candidateIds = model.getCandidates0(v)
    println(s"${candidateIds.length} candidates found")
    val t1 = System.currentTimeMillis()
    println(s"cost ${t1 - startTime}ms to get candidate ids")
    /** duplicate elements will be skipped here */
    val results = data.asInstanceOf[IndexedRDD[Long, Vector]].multifilter(v, r, candidateIds, L2)
    val t2 = System.currentTimeMillis()
    println(s"cost ${t2 - t1}ms to look up vectors")
    results
  }

  def RNNSearchV3(model: LSHModel, data: RDD[(Long, Vector)], v: Vector, r: Double = 20): Array[Long] = {
    val startTime = System.currentTimeMillis()
    val candidateIds = model.getCandidates0(v)
    println(s"${candidateIds.length} candidates found")
    val t1 = System.currentTimeMillis()
    println(s"cost ${t1 - startTime}ms to get candidate ids")
    /** duplicate elements will be skipped here */
    val candidateVectors = data.asInstanceOf[IndexedRDD[Long, Vector]].multiget(candidateIds)
    val results = candidateVectors.map(x => (x._1, L2(v, x._2))).filter(x => x._2 < r).keys.toArray
    val t2 = System.currentTimeMillis()
    println(s"cost ${t2 - t1}ms to look up vectors")
    results
  }

  def RNNSearchV2(model: LSHModel, data: RDD[(Long, Vector)], v: Vector, r: Double = 20): Array[Long] = {
    val startTime = System.currentTimeMillis()
    val candidateIds = model.getCandidates0(v)
//    val candidateIds = model.getCandidates1(v)
    println(s"${candidateIds.length} candidates found")
    val t1 = System.currentTimeMillis()
    println(s"cost ${t1 - startTime}ms to get candidate ids")
    /** duplicate elements will be skipped here */
    val candidateVectors = data.filter(x => candidateIds.contains(x._1)).collect()
//    val candidateVectors = data.asInstanceOf[IndexedRDD[Long, Vector]].multiget(candidateIds)
    val results = candidateVectors.map(x => (x._1, L2(v, x._2))).filter(x => x._2 < r).map(_._1)
//    val results = candidateVectors.map(x => (x._1, L2(v, x._2))).filter(x => x._2 < r).keys.toArray
    val t2 = System.currentTimeMillis()
    println(s"cost ${t2 - t1}ms to look up vectors")
    results
  }

  def RNNSearchV1(model: LSHModel, data: RDD[(Long, Vector)], v: Vector, r: Double = 20): Array[Long] = {
    val startTime = System.currentTimeMillis()
    val candidateIds = model.getCandidates1(v)
    println(s"${candidateIds.length} candidates found")
    val t1 = System.currentTimeMillis()
    println(s"cost ${t1 - startTime}ms to get candidate ids")
    /** duplicate elements will be skipped here */
    val candidateVectors = data.filter(x => candidateIds.contains(x._1)).collect()
    val results = candidateVectors.map(x => (x._1, L2(v, x._2))).filter(x => x._2 < r).map(x => x._1)
    val t2 = System.currentTimeMillis()
    println(s"cost ${t2 - t1}ms to look up vectors")
    results
  }
}
