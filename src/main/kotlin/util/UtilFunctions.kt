package util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.tdunning.math.stats.TDigest
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import logical.Expression
import logical.Group
import operators.BlockNLJoinOp
import operators.HashJoinOp
import operators.IndexNLJoinOp
import operators.JoinOp
import operators.LogicalOp
import operators.PhysicalOp
import operators.ScanOp
import operators.SortMergeJoinOp
import operators.SortOp
import org.bmstu.joins.ConditionOperator
import org.bmstu.util.TimingStats
import statistic.Catalog
import statistic.ColStats
import statistic.Stats

fun Expression.makePhysicalPlanPretty(indent: String = "", sb: StringBuilder = StringBuilder()): String {
    when (val op = this.op) {
        is ScanOp -> sb.append(indent).append("Scan(${op.table})").append("\n")
        is SortOp -> {
            sb.append(indent).append("Sort(${op.key})").append("\n")
            children[0].bestExpression!!.makePhysicalPlanPretty("$indent  ", sb)
        }
        is PhysicalOp -> {
            sb.append(indent).append(op.toString()).append("\n")
            if (children.size == 2) {
                children[0].bestExpression!!.makePhysicalPlanPretty("$indent  ", sb)
                children[1].bestExpression!!.makePhysicalPlanPretty("$indent  ", sb)
            }
        }
        else -> {
            // fallback на случай незнакомых операторов
            sb.append(indent).append(op.toString()).append("\n")
            children.forEach { child ->
                child.bestExpression?.makePhysicalPlanPretty("$indent  ", sb)
            }
        }
    }
    return sb.toString()
}

fun isEquality(op: ConditionOperator) = op == ConditionOperator.EQUALS

fun needsSorting(op: ConditionOperator) = op in setOf(
    ConditionOperator.LESS_THAN, ConditionOperator.LESS_THAN_OR_EQUALS,
    ConditionOperator.GREATER_THAN, ConditionOperator.GREATER_THAN_OR_EQUALS
)

fun dumpAllStats(statsMap: Map<String, Stats>, format: String = "yaml") {
    // создаём mapper для JSON или YAML
    val mapper = when (format.lowercase()) {
        "json" -> ObjectMapper().registerKotlinModule().writerWithDefaultPrettyPrinter()
        "yaml" -> ObjectMapper(YAMLFactory())
            .registerKotlinModule()
            .writerWithDefaultPrettyPrinter()
        else   -> error("Unsupported format: $format")
    }

    // папка для вывода
    val outDir = Paths.get("build", "stats").apply { Files.createDirectories(this) }
    for ((table, stats) in statsMap) {
        // finalise, если ещё не вызвали
        stats.finalizeStats()

        val outFile: Path = outDir.resolve("$table.$format")
        mapper.writeValue(outFile.toFile(), stats)
        println("Статистика для $table записана в ${outFile.toAbsolutePath()}")
    }
}

fun estimateRowSize(op: LogicalOp, children: List<Group>): Long {
    return when (op) {
        is ScanOp -> Catalog.stats(op.table).avgRowSize.toLong()
        is JoinOp,
        is HashJoinOp,
        is BlockNLJoinOp,
        is IndexNLJoinOp,
        is SortMergeJoinOp -> {
            children.sumOf { it.avgRowSize }
        }

        else -> 100L
    }
}


inline fun <T> accumulateTime(label: String, block: () -> T): T {
    val start = System.nanoTime()
    val result = block()
    val end = System.nanoTime()
    TimingStats.record(label, end - start)
    return result
}

private fun clone(cs: ColStats): ColStats = cs.copy(
    hll = cs.hll,      // если нужен deep-copy
    tdigest = TDigest.createDigest(100.0).also { d ->
        cs.tdigest.centroids().forEach { c ->
            repeat(c.count().toInt()) { d.add(c.mean()) }
        }
    },
    prefixCounts = cs.prefixCounts.toMutableMap()
)
