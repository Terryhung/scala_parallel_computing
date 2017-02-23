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
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

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

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    var right_paran = 0
    for (i <- chars){
      if (i == '(') {
        right_paran = right_paran + 1
      } else if (i == ')'){
        if (right_paran == 0){
          return false
        } else {
          right_paran = right_paran - 1
        }
      }
    }

    if (right_paran == 0) {
      true
    } else {
      false
    }
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      var _arg1 = arg1
      var _arg2 = arg2
      var _idx = idx
      while (_idx <= until) {
        if (chars(idx) == '('){
          _arg1 = _arg1 + 1
        } else if (chars(idx) == ')'){
          if (arg1 > 0){
            _arg1 = _arg1 - 1
          } else {
            _arg2 = _arg2 + 1
          }
        }
        _idx = _idx + 1
      }

      (_arg1, _arg2)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold){
        if (from == 0){
          if (balance(chars.slice(from, until)) == false){
            false
          }
        }
        traverse(from, until, 0, 0)
      }

      else {
        val m = from + (until - from) / 2
        var (l, r) = parallel(reduce(from, m), reduce(m, until))
        val min = math.min(l._1, r._2)
        (l._1 + r._1 - min, l._2 + r._2 - min)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
