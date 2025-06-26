package statistic

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDate
import kotlin.math.ln
import kotlin.math.pow
import util.HashUtil.xxHash32


class HyperLogLog(private val p: Int = 14) {
    private val m = 1 shl p
    private val M = ByteArray(m)
    private val alpha: Double = when (m) {
        16 -> 0.673
        32 -> 0.697
        64 -> 0.709
        else -> 0.7213 / (1 + 1.079 / m)
    }

    fun add(value: Any) {
        val data: ByteArray = when (value) {
            is Int -> ByteBuffer.allocate(4).putInt(value).array()
            is Long -> ByteBuffer.allocate(8).putLong(value).array()
            is Double -> ByteBuffer.allocate(8).putDouble(value).array()
            is BigDecimal -> {
                val unscaled = value.unscaledValue().toByteArray()
                val scaleBuf = ByteBuffer.allocate(4).putInt(value.scale()).array()
                unscaled + scaleBuf
            }

            is LocalDate -> {
                ByteBuffer.allocate(8).putLong(value.toEpochDay()).array()
            }

            is String -> value.toByteArray(Charsets.UTF_8)
            else -> value.toString().toByteArray(Charsets.UTF_8)
        }

        val x = xxHash32(data).toUInt()
        val j = (x shr (32 - p)).toInt()
        val w = x shl p
        val rho = w.countLeadingZeroBits() + 1
        if (M[j] < rho) M[j] = rho.toByte()
    }


    fun estimate(): Double {
        val sum = M.fold(0.0) { acc, v -> acc + 2.0.pow(-v.toInt()) }
        val E = alpha * m * m / sum

        if (E <= 2.5 * m) {
            val V = M.count { it.toInt() == 0 }
            if (V > 0) {
                return m * ln(m.toDouble() / V)
            }
        }

        val two32 = 2.0.pow(32)
        if (E > (1.0 / 30) * two32) {
            return -two32 * ln(1 - E / two32)
        }

        return E
    }
}