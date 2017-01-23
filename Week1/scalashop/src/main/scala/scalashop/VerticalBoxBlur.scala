package scalashop

import org.scalameter._
import common._

object VerticalBoxBlurRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 5,
    Key.exec.maxWarmupRuns -> 10,
    Key.exec.benchRuns -> 10,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val radius = 3
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqtime = standardConfig measure {
      VerticalBoxBlur.blur(src, dst, 0, width, radius)
    }
    println(s"sequential blur time: $seqtime ms")

    val numTasks = 32
    val partime = standardConfig measure {
      VerticalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }

}

/** A simple, trivially parallelizable computation. */
object VerticalBoxBlur {

  /** Blurs the columns of the source image `src` into the destination image
   *  `dst`, starting with `from` and ending with `end` (non-inclusive).
   *
   *  Within each column, `blur` traverses the pixels by going from top to
   *  bottom.
   */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    for {
      x <- from until end // for each column
      y <- 0 until src.height // going from top to bottom
      if (x >= 0 && x < src.width) // No need to check y as it is specifically defined
    } yield {
      val rgba = boxBlurKernel(src, x, y, radius)
      dst.update(x, y, rgba)
    }
  }

  /**
   * Blurs the columns of the source image in parallel using `numTasks` tasks.
   *
   *  Parallelization is done by stripping the source image `src` into
   *  `numTasks` separate strips, where each strip is composed of some number of
   *  columns.
   */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    // Get the numbers we need for parallelization
    val allColumns = 0 until src.width // indexes of all the columns in the image
    val columnsPerTask = math.max(src.width / numTasks, 1) // make sure we have atleast 1 column
    val startColumns = allColumns by columnsPerTask // cols where we split for starting separate tasks

    // parallelization
    val allTasks = startColumns.map(elem => {
      task {
        // do elem to (elem + columnsPerTask) columns in one task
        blur(src, dst, elem, elem + columnsPerTask, radius)
      }
    })

    // finally join
    allTasks.map(task => task.join()) // each element of allTasks is a task
  }

}
