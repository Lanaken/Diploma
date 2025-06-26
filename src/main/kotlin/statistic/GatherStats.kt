package statistic

import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate.parse
import kotlin.math.ceil
import kotlin.sequences.forEach
import org.bmstu.joins.ConditionOperator
import org.bmstu.reader.CType
import org.bmstu.reader.YSchema

private const val OBJ_HEADER = 16
private const val REF_SIZE = 8
private const val STRING_OVH  = 24
private const val BIGDEC_OVH  = 48
const val PREFIX_LEN = 3

fun gatherStats(tbl: Path, schema: YSchema): Stats {
    val st = Stats()
    var sampled = 0
    var bytesAccum = 0L
    Files.newBufferedReader(tbl).useLines { lines ->

        lines.forEach { line ->
            st.rows++
            val fields = line.split('|')


            for ((i, col) in schema.columns.withIndex()) {
                val f = fields[i]
                val cs = st.col.computeIfAbsent(col.name) { ColStats(col.type) }

                when (col.type) {
                    CType.INT, CType.BIGINT -> {
                        if (f.isEmpty()) {
                            cs.nullCnt++
                            continue
                        }
                        val v = f.toLong()
                        cs.prevLong?.let { if (cs.wasSorted && v < it) cs.wasSorted = false }
                        cs.prevLong = v
                        cs.hll.add(v)
                        cs.tdigest.add(v.toDouble())
                        if (v < cs.min) cs.min = v
                        if (v > cs.max) cs.max = v
                        bytesAccum += 4
                    }

                    CType.DECIMAL -> {
                        if (f.isEmpty()) {
                            cs.nullCnt++
                            continue
                        }
                        val bd = BigDecimal(f)
                        cs.prevDecimal?.let { if (cs.wasSorted && bd < it) cs.wasSorted = false }
                        cs.prevDecimal = bd
                        cs.hll.add(bd)
                        cs.tdigest.add(bd.toDouble())
                        if (cs.minDec == null || bd < cs.minDec!!) cs.minDec = bd
                        if (cs.maxDec == null || bd > cs.maxDec!!) cs.maxDec = bd
                        bytesAccum += BIGDEC_OVH
                    }

                    CType.DATE -> {
                        if (f.isEmpty()) {
                            cs.nullCnt++
                            continue
                        }
                        val epoch = parse(fields[i]).toEpochDay()
                        cs.prevEpochDate?.let { if (cs.wasSorted && epoch < it) cs.wasSorted = false }
                        cs.prevEpochDate = epoch
                        cs.hll.add(epoch)
                        if (epoch < cs.min) cs.min = epoch
                        if (epoch > cs.max) cs.max = epoch
                        bytesAccum += 8
                    }

                    CType.CHAR, CType.STRING -> {
                        if (f.isEmpty()) {
                            cs.nullCnt++
                            continue
                        }
                        cs.prevStr?.let { if (cs.wasSorted && f < it) cs.wasSorted = false }
                        cs.prevStr = f
                        cs.hll.add(f)
                        if (cs.minStr == null || f < cs.minStr!!) cs.minStr = f
                        if (cs.maxStr == null || f > cs.maxStr!!) cs.maxStr = f
                        bytesAccum += STRING_OVH + 2 * f.length
                    }
                }
            }
            bytesAccum += OBJ_HEADER + REF_SIZE * schema.columns.size
            //if (++sampled == 50000) return@forEach
        }
    }

    st.rowSizeBytes = (bytesAccum / sampled.coerceAtLeast(1))
    val bytes = Files.size(tbl)
    st.filePages = ceil(bytes / 8192.0).toLong()

    st.col.values.forEach { cs ->
        cs.ndv = cs.hll.estimate().toLong()
    }
    st.totalBytes = bytesAccum
    st.finalizeStats()
    return st
}

fun selectivityByTDigest(
    op: ConditionOperator,
    left: ColStats?,
    right: ColStats?
): Double {
    if (left == null || right == null) return 1.0/3
    return when (op) {
        ConditionOperator.LESS_THAN -> left.tdigest.cdf(right.min.toDouble())
        ConditionOperator.GREATER_THAN -> 1.0 - left.tdigest.cdf(right.max.toDouble())
        ConditionOperator.LESS_THAN_OR_EQUALS ->
            left.tdigest.cdf(right.max.toDouble())
        ConditionOperator.GREATER_THAN_OR_EQUALS ->
            1.0 - left.tdigest.cdf(right.min.toDouble())
        else -> 1.0/3
    }
}