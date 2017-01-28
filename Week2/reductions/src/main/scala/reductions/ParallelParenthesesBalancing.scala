package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true) withWarmer (new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /**
   * Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceArray(chars: Array[Char], count: Int): Boolean = { // positive count open (
      if (chars.isEmpty) if (count == 0) true else false
      else if (chars.head == '(') balanceArray(chars.tail, count + 1)
      else if (chars.head == ')') {
        if (count - 1 >= 0) balanceArray(chars.tail, count - 1)
        else false
      } else balanceArray(chars.tail, count)
    }
    balanceArray(chars, 0)
  }

  /**
   * Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(fromIdx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      // In the helper, the count becomes a pair as we traverse 2 separate streams/trees
      def traverseArray(chars: Array[Char], count: (Int, Int)): (Int, Int) = {
        if (chars.isEmpty) count
        else if (chars.head == '(') traverseArray(chars.tail, (count._1 + 1, count._2))
        else if (chars.head == ')') {
          if (count._1 >= 0) traverseArray(chars.tail, (count._1 - 1, count._2))
          else traverseArray(chars.tail, (count._1, count._2 + 1))
        } else traverseArray(chars.tail, count)
      }
      traverseArray(chars.slice(fromIdx, until), (arg1, arg2))
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = (from + until) / 2
        val (left, right) = parallel(reduce(from, mid), reduce(mid, until))
        val matched = scala.math.min(left._1, left._2)
        (left._1 + right._1 - matched, left._2 + right._2 - matched)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
