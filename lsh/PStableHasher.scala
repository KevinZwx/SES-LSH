package lsh

import scala.util.Random
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

/**
 * generates hash value for the given vector
 * @param a randomly generated vector, each item of which is a Gaussian random variable
 * @param b double number randomly drawn from [0, w]
 * @param w binLength
 */
class PStableHasher(val a: Vector, val b: Double, val w: Double = 4.0) extends Serializable{

  def hash(v: Vector): Int = {
    Math.floor((L2.dotProduct(a, v) + b) / w).toInt
  }
}

object PStableHasher{

  final val PRIME = Math.pow(2, 31) -5
  final val MAX = Math.pow(2, 29).toInt

  def apply(dim: Int = 1, binLength: Double = 4.0, seed: Long = System.nanoTime()) = new PStableHasher(drawA(dim, seed), drawB(binLength, seed), binLength)

  def drawA(dim: Int, seed: Long): Vector = {
    val r = new Random(seed)
//    val vecArray = (for(i <- 1 to dim) yield r.nextGaussian()).toArray
//    val index = (0 to vecArray.length -1).toArray
//    Vectors.sparse(dim, index, vecArray)
    Vectors.dense((for(i <- 1 to dim) yield r.nextGaussian()).toArray)
  }

  def drawB(binLength: Double, seed: Long): Double = {
    val r = new Random(seed)
    r.nextDouble() * binLength
  }

  def h2(histogram: List[Int], seed: Long = System.nanoTime()): Long = {

    val r = new Random(seed)

    var sum: Long = 0
    for (i <- histogram)
      sum += i*(r.nextInt(MAX-1)+1)

    (sum % PRIME).toLong
  }

  def BKDRHash(key: String): Int={

    val seed = 31
    var hash = 0

//    for (i <- key){
//      print(i + " ")
//      hash = (hash * seed) + i
//    }
    key.foreach(i => hash = (hash * seed) + i)
    println()
    hash & 0x7FFFFFFF
  }

  def bucketHash(key: (Int, String), numPartitions: Int): Int = {
    if (key.hashCode() > 0) {
      key.hashCode % numPartitions
    }else {
      (key.hashCode % numPartitions) + numPartitions
    }
  }
}