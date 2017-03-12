package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import stackoverflow.StackOverflow.sc

import scala.collection.immutable.IndexedSeq

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  val Q = 1
  val A = 2

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("grouped postings") {
    val question = Posting(Q, 1, None, None, 1, Some("Java"))
    val answer = Posting(A, 2, None, Some(1), 1, Some("Java"))
    val postings = sc.parallelize(Seq(question, answer))
    val grouped = testObject.groupedPostings(postings).collect()
    assert(grouped != null)
    assert(grouped.length == 1)
    assert(grouped(0)._1 == 1)
    val ps: IndexedSeq[(Posting, Posting)] = grouped(0)._2.toIndexedSeq
    assert(ps.length == 1)
    assert(ps.head == (question, answer))
  }

  test("scored postings") {
    val question1 = Posting(Q, 1, None, None, 1, Some("Java"))
    val answer1_q1 = Posting(A, 2, None, Some(1), 2, Some("Java"))
    val answer2_q1 = Posting(A, 25, None, Some(1), 25, Some("Java"))
    val questions2 = Posting(Q, 3, None, None, 3, Some("Scala"))
    val answer1_q2 = Posting(A, 4, None, Some(3), 4, Some("Scala"))
    val answer2_q2 = Posting(A, 45, None, Some(3), 45, Some("Scala"))
    val grouped: RDD[(Int, Iterable[(Posting, Posting)])] = sc.parallelize(
      Seq(
        (1, Seq((question1, answer1_q1), (question1, answer2_q1))),
        (3, Seq((questions2, answer1_q2), (questions2, answer2_q2)))
      ))
    val scored: Array[(Posting, Int)] = testObject.scoredPostings(grouped).collect().sortBy(p => p._1.id)
    assert(scored != null)
    assert(scored.length == 2)
    val (pos1, score1) = scored(0)
    val (pos2, score2) = scored(1)
    assert(pos1 == question1)
    assert(pos2 == questions2)
    assert(score1 == 25)
    assert(score2 == 45)
  }

}
