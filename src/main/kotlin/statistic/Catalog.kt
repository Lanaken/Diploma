package statistic

import com.tdunning.math.stats.TDigest
import java.math.BigDecimal
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
import logical.Group
import org.bmstu.indexes.BptHeader
import org.bmstu.joins.ConditionOperator
import org.bmstu.reader.CType


data class ColStats(
    var kind: CType,
    var ndv: Long = 0,
    var min: Long = Long.MAX_VALUE,
    var max: Long = Long.MIN_VALUE,
    var nullCnt: Long = 0,
    var minStr: String? = null,
    var maxStr: String? = null,
    var minDec: BigDecimal? = null,
    var maxDec: BigDecimal? = null,
    var wasSorted: Boolean = true,
    val hll: HyperLogLog = HyperLogLog(14),
    var prevLong: Long? = null,
    var prevStr: String? = null,
    var prevEpochDate: Long? = null,
    var prevDecimal: BigDecimal? = null,
    val tdigest: TDigest = TDigest.createDigest(100.0),
    val prefixCounts: MutableMap<String, Long> = mutableMapOf(),
)

data class Stats(
    var rows: Long = 0,
    var filePages: Long = 0,
    var totalBytes: Long = 0,
    var rowSizeBytes: Long = 0,
    val col: MutableMap<String, ColStats> = mutableMapOf()
) : java.io.Serializable {
    var avgRowSize: Double = 0.0; private set
    fun finalizeStats() {
        avgRowSize = if (rows == 0L) 0.0 else totalBytes.toDouble() / rows
    }
}

data class IndexMeta(val leafPages: Long, val height: Int): java.io.Serializable

object Catalog {

    /* ---------- helper: нормализуем «имя» набора таблиц -------------- */
    private fun keyOf(tables: Set<String>): String =
        tables.sorted().joinToString("&")        // одиночная таблица = простое имя

    /* ---------- статистика таблиц и результатов соединений ----------- */
    private val tbl = ConcurrentHashMap<String, Stats>()          // key → Stats

    fun stats(table: String): Stats = tbl.getValue(table)
    fun putStats(table: String, s: Stats) {
        tbl[table] = s
    }

    /** Унифицированные методы для *произвольного* набора таблиц */
    fun stats(tables: Set<String>): Stats = tbl.getValue(keyOf(tables))
    fun putStats(tables: Set<String>, s: Stats) {
        tbl[keyOf(tables)] = s
    }

    /* ---------- метаданные индексов ---------------------------------- */
    private val idx = ConcurrentHashMap<Pair<String, String>, IndexMeta>()
    private val idxPath = ConcurrentHashMap<Pair<String, String>, Path>()

    fun putIndex(table: String, col: String, meta: IndexMeta) {
        idx[table to col] = meta
    }

    fun index(table: String, col: String): IndexMeta? = idx[table to col]

    fun putIndexPath(table: String, col: String, path: Path) {
        idxPath[table to col] = path
    }

    fun getIndexPath(table: String, col: String): Path? = idxPath[table to col]
}


fun Group.hasIndexFor(col: String): Boolean =
    tables.singleOrNull()?.let { Catalog.index(it, col) != null } ?: false

fun synthesizeJoinStats(
    left: Stats,
    right: Stats,
    joinRows: Long
): Stats {
    val st = Stats()
    st.rows         = joinRows
    st.rowSizeBytes = (left.avgRowSize + right.avgRowSize).toLong()
    st.totalBytes   = st.rows * st.rowSizeBytes
    st.filePages    = ceil(st.totalBytes / 8192.0).toLong()
    st.finalizeStats()
    return st
}

fun loadIndexMeta(idxPath: Path, table: String, col: String) {
    FileChannel.open(idxPath, StandardOpenOption.READ).use { ch ->
        val hdr = BptHeader.read(ch)
        val height = ceil(
            ln(hdr.pagesUsed.toDouble()) / ln(129.0)
        ).toInt().coerceAtLeast(1)
        Catalog.putIndex(table, col, IndexMeta(hdr.pagesUsed, height))
        Catalog.putIndexPath(table, col, idxPath)
    }
}


