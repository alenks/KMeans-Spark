import Assignment2.{rawPostings, sc}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.Row

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int,
                   id: Int,
                   parentId: Option[Int],
                   score: Int,
                   tags: Option[String])
  extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  //sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile(args(0))
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000
  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop*/
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
        score =          arr(3).toInt,
        tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })

  /** Group the questions and answers together */
  def groupedPostings(raw : RDD[Posting]): RDD[(Int, (Int, Int))] = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QID value in the first element of a tuple.

    val QuestionRDD = raw.filter(r => r.postingType == 1).map(r => (r.id, (Assignment2.Domains.indexOf(r.tags.get), r.score)))
    // Question RDD - (QID, (D, S_Q))

    val AnswerRDD = raw.filter(r => r.postingType == 2).map(r => (r.parentId.get, (r.id, r.score)))
    // Answer RDD - (QID, (AID, S_A))

    QuestionRDD.join(AnswerRDD)
      .map(rec =>(rec._1, (rec._2._1._1, rec._2._2._2)))
    // (QID, ((D, S_Q), (AID, S_A)) ==> (QID, (D, S_A))
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(grouped : RDD[(Int, (Int, Int))]) : RDD[(Int, (Int, Int))] = {

    def seqOp = (accumulator: (Int, Int), element: (Int, Int)) =>
      if(accumulator._2 > element._2) accumulator else element

    def combOp = (accumulator1: (Int, Int), accumulator2: (Int, Int)) =>
      if(accumulator1._2 > accumulator2._2) accumulator1 else accumulator2

    val zeroVal = (0, 0)

    grouped.map(t => (t._1, (t._2._1, t._2._2)))
      .aggregateByKey(zeroVal)(seqOp, combOp)
    // (QID, (D, S_A)) ==> (QID, (D, max(S_A)))

  }

  /** Compute the vectors for the kmeans */
  def vectorPostings(scored : RDD[(Int, (Int, Int))]) : RDD[(Int, Int)] = {

    scored.map(r => (r._2._1*Assignment2.DomainSpread, r._2._2))
    // (QID, (D, max(S_A))) ==> (D*DomainSpread, max(S_A))

  }

  def sampleVectors(vectors : RDD[(Int, Int)]) : Array[(Int, Int)] = {
    vectors.takeSample(withReplacement = false, kmeansKernels)
  }




  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], debug: Boolean) : RDD[(Int, Iterable[(Int, Int)])] = {

    var tempDist = 50.0D
    //println("initial vectors")
    //means.foreach(println)

    while(!converged(tempDist)) {
      val closest = vectors.map (p => (findClosest(p, means), p))

      //println("new pointStats vectors")
      val pointStats = closest.groupByKey()
      //pointStats.foreach(println)

      val newPoints = pointStats.map  {pair => (pair._1, averageVectors(pair._2))}.collectAsMap()
      //newPoints.foreach(println)

      tempDist = 0.0D
      for (i <- 0 until kmeansKernels) {
      //for (i <- 0 until newPoints.size) {
        //println(i, newPoints(i), means(i))
        if (means.isDefinedAt(i) &&  newPoints.isDefinedAt(i)) {
          tempDist += euclideanDistance(means(i), newPoints(i))
      }
      }

      for (newP <- newPoints) {
        means(newP._1) = newP._2
      }
      println(s"Finished iteration (delta = $tempDist)")

    }
    val closest = vectors.map (p => (findClosest(p, means), p))
    val pointStats = closest.groupByKey()
    pointStats.foreach(println)

    pointStats
  }

  //
  //
  //  Kmeans utilities (Just some cases, you can implement your own utilities.)
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  def clusterResults(means: RDD[(Int, Iterable[(Int, Int)])], vectors: RDD[(Int, Int)]) : scala.collection.Map[Int, (Int, (Int,Int), Int)] = {
    means.map  {pair => (pair._1,  (pair._2.size, averageVectors(pair._2), computeMedian(pair._2)))}.collectAsMap()
  }

  //  Displaying results:

  def printResults(results : scala.collection.Map[Int, (Int, (Int,Int), Int)]) : Unit = {
    results.foreach { case (id: Int, (size: Int, centroid: (Int, Int), median: Int)) =>
      println("%s %d %s %d".format(centroid, size, centroid._2, median)) }
  }
}