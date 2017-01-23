
import common._

package object scalashop {

  /** The value of every pixel is represented as a 32 bit integer. */
  type RGBA = Int

  /** Returns the red component. */
  def red(c: RGBA): Int = (0xff000000 & c) >>> 24

  /** Returns the green component. */
  def green(c: RGBA): Int = (0x00ff0000 & c) >>> 16

  /** Returns the blue component. */
  def blue(c: RGBA): Int = (0x0000ff00 & c) >>> 8

  /** Returns the alpha component. */
  def alpha(c: RGBA): Int = (0x000000ff & c) >>> 0

  /** Used to create an RGBA value from separate components. */
  def rgba(r: Int, g: Int, b: Int, a: Int): RGBA = {
    (r << 24) | (g << 16) | (b << 8) | (a << 0)
  }

  /** Restricts the integer into the specified range. */
  def clamp(v: Int, min: Int, max: Int): Int = {
    if (v < min) min
    else if (v > max) max
    else v
  }

  /** Image is a two-dimensional matrix of pixel values. */
  class Img(val width: Int, val height: Int, private val data: Array[RGBA]) {
    def this(w: Int, h: Int) = this(w, h, new Array(w * h))
    def apply(x: Int, y: Int): RGBA = data(y * width + x)
    def update(x: Int, y: Int, c: RGBA): Unit = data(y * width + x) = c
  }

  /** Computes the blurred RGBA value of a single pixel of the input image. */
  def boxBlurKernel(src: Img, x: Int, y: Int, radius: Int): RGBA = {

    // Get the lower and upper limits to traverse through the pixel matrix.
    // We need to use clamp here so that we don't cross the first/last row or column
    // of the src image i.e. so that we stay within the image.
    val xMin = clamp(x - radius, 0, src.width - 1)
    val yMin = clamp(y - radius, 0, src.height - 1)
    val xMax = clamp(x + radius, 0, src.width - 1)
    val yMax = clamp(y + radius, 0, src.height - 1)

    // initialize the variables
    var (i, j) = (xMin, yMin)
    var totalPixels = 0 // to keep a track of no. of pixels to calc the average.
    var (r, g, b, a) = (0, 0, 0, 0)

    while (i <= xMax) {
      while (j <= yMax) {
        val rgba = src(i, j) // current pixel RGBA
        r += red(rgba)
        g += green(rgba)
        b += blue(rgba)
        a += alpha(rgba)

        j += 1
        totalPixels += 1
      }
      i += 1
      j = yMin // reset j for each i
    }

    rgba(r / totalPixels, g / totalPixels, b / totalPixels, a / totalPixels)
  }

}
