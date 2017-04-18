package lsh
//import breeze.linalg.BitVector
import org.apache.spark.mllib.linalg.Vector

/**
 * interface defining similarity measurement between 2 vectors
 */
trait VectorDisctance extends Serializable {
  def apply(vecA: Vector, vecB: Vector): Double

  def dotProduct(vecA: Vector, vecB: Vector): Double = {
    val vecAarray = vecA.toArray
    val vecBarray = vecB.toArray
    vecAarray.view.zip(vecBarray.view).map { case (a, b) => a * b}.sum
  }

  /**
  def dotProduct(vecA: SparseVector, vecB: SparseVector): Double = {

    println("dotproduct2 is called")

    val indexA = vecA.indices
    val indexB = vecB.indices
    val minSize = Math.min(indexA.length, indexB.length)
    var sum = 0

    for (i <- 0 until minSize)
      if (indexA(i) == indexB(i))
        sum += vecA.values(indexA(i)) * vecB.values(indexB(i))
    sum
//    val vecAarray = vecA.toArray
//    val vecBarray = vecB.toArray
//    vecAarray.view.zip(vecBarray.view).map { case (a, b) => a * b}.sum
  }
    **/
}

/**
 * implementation of [[VectorDisctance]] that computes L2 norm similarity
 * between two vectors
 */
object L2 extends VectorDisctance{

//  def dotProduct(vecA: Vector, vecB: Vector): Double = {
//
//    var sum: Double = 0
//    val sparseA = vecA.toSparse
//    val sparseB = vecB.toSparse
////    val index = {
////      if (sparseA.indices.length <= sparseB.indices.length) {
////        sparseA.indices
////      } else {
////        sparseB.indices
////      }
//    val index = vecA.toSparse.indices
//
////    val flags = BitVector(Math.max(vecA.size,vecB.size))
//    val flags = new Array[Int](Math.max(vecA.size, vecB.size))
//    for (i <- index){
//      if (flags(i) == 1){
//        sum += vecA(i) * vecB(i)
//      }
//    }
//  }

  def apply(vecA: Vector, vecB: Vector): Double = {
    Math.sqrt(vecA.toArray.zip(vecB.toArray).map(x => Math.pow(x._1 - x._2, 2)).sum)
  }

  /**
  def apply(vecA: SparseVector, vecB: SparseVector): Double = {

    Math.sqrt(dotProduct(vecA, vecB))
  }
  **/
}

/**
 * implementation of [[VectorDisctance]] that computes cosine similarity
 * between two vectors
 */
object Cosine extends VectorDisctance {

  def apply(vecA: Vector, vecB: Vector): Double = {
    dotProduct(vecA, vecB) / Math.sqrt(vecA.toArray.map(x => Math.pow(x, 2)).sum * vecB.toArray.map(x => Math.pow(x, 2)).sum)
  }
}

/**
 * implementation of [[VectorDisctance]] that computes jaccard similarity
 * between two vectors
 */
object Jaccard extends VectorDisctance {

    def apply(vecA: Vector, vecB: Vector): Double = {
      dotProduct(vecA, vecB) / (L2(vecA, vecA) + L2(vecB, vecB) - dotProduct(vecA, vecB))
//      dotProduct(vecAarray, vecBarray) / (dotProduct(vecAarray, vecAarray) + dotProduct(vecBarray, vecBarray) - dotProduct(vecAarray, vecBarray))
    }

}



