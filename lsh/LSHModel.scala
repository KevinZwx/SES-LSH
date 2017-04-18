package lsh

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 *
 * @param numHashFunctions number of hash functions
 * @param numHashTables number of hash tables
 * @param dimension dimension of input data
 * @param binLength length of bins
 */
class LSHModel(val numHashFunctions: Int, val numHashTables: Int, val dimension: Int, val binLength: Double)
  extends Serializable with Saveable {

  val bits = mutable.BitSet.empty

  /** only compute k/2 * m functions to reduce time complexity of hash function computation */
  val m = ((1 + Math.sqrt(1 + numHashTables << 3)) / 2).toInt
  val halfK = numHashFunctions >>> 1
  println(s"m:$m")

  private val _hashFunctions = ListBuffer[PStableHasher]()
//  private val _hashFunctions = ListBuffer[CosineHasher]()

  //  for (i <- 0 to numHashFunctions * numHashTables - 1)
  for (i <- 0 until halfK * m)
    _hashFunctions += PStableHasher(dimension, binLength)
//    _hashFunctions += CosineHasher(dimension)
  final var hashFunctions: List[(PStableHasher, Long)] = _hashFunctions.toList.zipWithIndex.map(x => (x._1, x._2.toLong))
//  final var hashFunctions: List[(CosineHasher, Long)] = _hashFunctions.toList.zipWithIndex.map(x => (x._1, x._2.toLong))

  /** hashTables ((tableId, hashKey), vectorId) */
//  var hashTables: IndexedRDD[String, List[Long]] = null
  var hashTables: RDD[(String, List[Long])] = null

  /** computes hash value for a vector in each hashTable. Array(tableID, binID) */
  def hashValue(data: Vector): Array[String] = {
    val values = hashFunctions.map(f => (f._2 % m, f._1.hash(data)))
      .groupBy(_._1)
      .map(a => (a._1, a._2.map(_._2).mkString(""))).toList

    var result = ListBuffer[String]()
    for (i <- values.indices) {
      for (j <- (i + 1) until values.length) {
        result += (values(i)._2 + values(j)._2)
      }
    }
    result.toArray.zipWithIndex.map(x => x._2 + x._1)
//    result.toList.zipWithIndex.map(x => (x._2 + x._1).hashCode.toLong)
    //    result.toList.zipWithIndex.map(x => Hasher.FNVHash1(x._2 + x._1))
  }


  /** returns candidates for a given vector with duplicates
    *
    * @param vec  query point(vector)
    * @return   candidate points of vec
    * */
  def getCandidates0(vec: Vector): Array[Long] = {
    val buckets = hashValue(vec)
    val candidates = hashTables.asInstanceOf[IndexedRDD[String, List[Long]]].multiget(buckets).flatMap(x => x._2).toArray.distinct
    candidates
  }

  def getCandidates1(vec: Vector): Array[Long] = {
    val buckets = hashValue(vec)
    val candidates = hashTables.filter(x => buckets.contains(x._1)).flatMap(x => x._2).distinct().collect()
    candidates
  }

  override def save(sc: SparkContext, path: String): Unit = {
    LSHModel.SaveLoadV0_0_1.save(sc, this, path)
  }

  override protected def formatVersion: String = "0.1"
}

object LSHModel extends Loader[LSHModel] with Logging {

  def load(sc: SparkContext, path: String): LSHModel = {
    LSHModel.SaveLoadV0_0_1.load(sc, path)
  }

  private[lsh] object SaveLoadV0_0_1 {

    //    private val thisFormatVersion = "0.0.1"
    //    private val thisClassName = this.getClass.getName()

    def thisFormatVersion: String = "0.1"

    def thisClassName: String = this.getClass.getName

    def save(sc: SparkContext, model: LSHModel, path: String): Unit = {

      // save metadata in json format
      val metadata =
        compact(render(
          ("class" -> thisClassName)
            ~ ("version" -> thisFormatVersion)
            ~ ("numHashFunctions" -> model.numHashFunctions)
            ~ ("numHashTables" -> model.numHashTables)
            ~ ("dimension" -> model.dimension)
            ~ ("binLength" -> model.binLength)))

      //save metadata info
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      //save hash functions as (functionIndex, RandomVectorA, RandomNumberB, binLength, dimension)
      sc.parallelize(model.hashFunctions
        .map(f => (f._2, f._1.a.toArray.mkString(","), f._1.b))) //(table#, a, b)
//        .map(f => (f._2, f._1.r.mkString(",")))) //(table#, a, b)
        .map(_.productIterator.mkString("\t"))
        .saveAsTextFile(Loader.hasherPath(path))

      //save data as (hashTableId#+hashValue, vectorId)
      model.hashTables
        .map(x => x._1 + "\t" + x._2.mkString(","))
//        .map(x => x._1 + "\t" + x._2.productIterator.mkString("\t"))
//        .map(_.productIterator.mkString("\t"))
        .saveAsTextFile(Loader.dataPath(path))
    }

    def load(sc: SparkContext, path: String): LSHModel = {

      implicit val formats = DefaultFormats
      val (className, formatVersion, numHashFunctions, numHashTables, dimension, binLength, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)
      assert(numHashTables != 0, s"Loaded hashTables are empty")
      assert(numHashFunctions != 0, s"Loaded hashFunctions are empty")
      assert(dimension != 0, s"Loaded binLength is 0")
      assert(binLength != 0, s"Loaded binLength is 0")

      //load hashTables
//      val hashTables_ = IndexedRDD(sc.textFile(Loader.dataPath(path), 256)
//        .map(x => x.split("\t"))
////        .map(x => (x(0).toLong, x(1).split(",").map(a => (x(0).toLong, a.toLong))))
////        .flatMap(_._2))
//        .map(x => (x(0), x(1).split(",").map(_.toLong).toList)))

      val hashTables_ = sc.textFile(Loader.dataPath(path), 128)
        .map(x => x.split("\t"))
//        .map(x => (x(0).toLong, x(1).split(",").map(a => (x(0).toLong, a.toLong))))
//        .flatMap(_._2))
        .map(x => (x(0), x(1).split(",").map(_.toLong).toList))

      //load hashFunctions
      val hashFunctions_ = sc.textFile(Loader.hasherPath(path))
        .map(a => a.split("\t"))
        .map { x =>
          val functionIndex = x(0).toLong
          val a = Vectors.dense(x(1).split(",").map(y => y.toDouble))
          val b = x(2).toDouble
//          val r = x(1).split(",").map(_.toDouble)
          val hasher = new PStableHasher(a, b, binLength)
//          val hasher = new CosineHasher(r)
          (hasher, functionIndex)
        }.collect()

      // compute parameters for LSHModel according to hashTables and hashFunctions
      //      val numHashTables: Int = hashTables.map(x => x._1._1).distinct().count().toInt
      //      val numHashTables: Int = hashTables.map(x => x._1).distinct().count().toInt
      //      val numHashFunctions: Int = hashFunctions.length / numHashTables
      //      val dimensions = hashFunctions.head._1.a.size
      //      val binLength = hashFunctions.head._1.w

      //Validate loaded data
      //check size of data

      //check hashValue size. Should be equal to numHashFunc
      //      val hashKeyLength = hashTables.collect().head._1._2.length
      //      val hashKey1 = hashTables.collect().head._1._2
      //      println(s"numhashFunctions: ${numHashFunctions}, hahsKey: ${hashKey1} hashKeyLength: ${hashKeyLength}")
      //      assert(hashTables.map(x => x._1._2).filter(x => x.size != numHashFunctions).collect().size == 0,
      //        s"hashValues in data does not match with hash functions")

      //create model
      val model = new LSHModel(numHashFunctions, numHashTables, dimension, binLength)
      model.hashFunctions = hashFunctions_.toList
      model.hashTables = hashTables_
      model
    }
  }

  /** transform an array to a sparse vector
    *  @deprecated
    * */
  def ArrayToSparseVector(arr: Array[Double]): Vector = {
    val size = arr.length
    val arrWithIndex = arr.zipWithIndex
    val index: Array[Int] = arrWithIndex.filter(_._1 != 0).unzip._2.toArray
    val number: Array[Double] = arrWithIndex.filter(_._1 != 0).unzip._1.toArray
    Vectors.sparse(size, index, number)
  }
}

/** Helper functions for save/load data from mllib package.
  * TODO: Remove and use Loader functions from mllib. */
private[lsh] object Loader {

  /** Returns URI for path/data using the Hadoop filesystem */
  def dataPath(path: String): String = new Path(path, "hashTables").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def metadataPath(path: String): String = new Path(path, "metadata").toUri.toString

  /** Returns URI for path/metadata using the Hadoop filesystem */
  def hasherPath(path: String): String = new Path(path, "hashFunctions").toUri.toString

  /**
   * Load metadata from the given path.
   * @return (class name, version, metadata)
   */
  def loadMetadata(sc: SparkContext, path: String): (String, String, Int, Int, Int, Double, JValue) = {
    implicit val formats = DefaultFormats
    val metadata = parse(sc.textFile(metadataPath(path)).first()) //parse json to scala object, here is a path string?
    val clazz = (metadata \ "class").extract[String]
    val version = (metadata \ "version").extract[String]
    val numHashFunctions = (metadata \ "numHashFunctions").extract[Int]
    val numHashTables = (metadata \ "numHashTables").extract[Int]
    val dimension = (metadata \ "dimension").extract[Int]
    val binLength = (metadata \ "binLength").extract[Double]
    (clazz, version, numHashFunctions, numHashTables, dimension, binLength, metadata)
  }
}