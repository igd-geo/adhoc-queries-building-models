import kotlin.math.pow

object MortonCode {

    fun encode(x: Float, y: Float, precision: Int): Long {
        val xi = convertToInt(x, precision)
        val yi = convertToInt(y, precision)
        return encode(xi, yi)
    }

    fun encode(x: Int, y: Int): Long {
        return (interleave(y) shl 1) + interleave(x)
    }

    fun decode(index: Long, precision: Int): Pair<Float, Float> {
        val (xi, yi) = deinterleave(index)
        val x = xi.toFloat() / 10.0f.pow(precision)
        val y = yi.toFloat() / 10.0f.pow(precision)

        return Pair(x, y)
    }

    private fun convertToInt(value: Float, precision: Int): Int {
        return (value * 10.0.pow(precision)).toInt()
    }

    private fun interleave(x: Int): Long {
        var xi: Long = x.toLong()
        xi = xi and 0x00000000ffffffffL

        xi = (xi xor (xi shl 16)) and 0x0000ffff0000ffffL
        xi = (xi xor (xi shl 8))  and 0x00ff00ff00ff00ffL
        xi = (xi xor (xi shl 4))  and 0x0f0f0f0f0f0f0f0fL
        xi = (xi xor (xi shl 2))  and 0x3333333333333333L
        xi = (xi xor (xi shl 1))  and 0x5555555555555555L
        return xi
    }

    private fun deinterleave(interl: Long): Pair<Int, Int> {
        var x: Long = interl           and 0x5555555555555555L
        var y: Long = (interl shr 1)   and 0x5555555555555555L

        x = (x or (x shr 1))    and 0x3333333333333333L
        x = (x or (x shr 2))    and 0x0f0f0f0f0f0f0f0f
        x = (x or (x shr 4))    and 0x00ff00ff00ff00ffL
        x = (x or (x shr 8))    and 0x0000ffff0000ffffL
        x = (x or (x shr 16))   and 0x00000000ffffffffL

        y = (y or (y shr 1))    and 0x3333333333333333L
        y = (y or (y shr 2))    and 0x0f0f0f0f0f0f0f0fL
        y = (y or (y shr 4))    and 0x00ff00ff00ff00ffL
        y = (y or (y shr 8))    and 0x0000ffff0000ffffL
        y = (y or (y shr 16))   and 0x00000000ffffffffL

        return Pair(x.toInt(),y.toInt())
    }


}