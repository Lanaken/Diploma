package cost

import kotlin.math.ceil
import kotlin.math.ln
import kotlin.math.log2
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt
import logical.Group
import operators.*
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.algorithms.PartitionHashJoinIterator
import org.bmstu.reader.CType
import org.bmstu.util.MemoryUtil
import statistic.*

const val PAGE_BYTES = 8192.0
const val C_IO  = 1.0
private const val C_CPU = 0.01
const val C_IO_INDEX  = 0.75 * C_IO
const val C_CPU_PARSE = 0.002


const val HASH_BUILD_CPU  = 3 * C_CPU
const val HASH_PROBE_CPU  = 2 * C_CPU

const val MERGE_CPU = 1 * C_CPU
const val COMPARE_CPU = 1 * C_CPU
private const val BUILD_BATCH_SIZE = 500

private val MEM_BYTES: Double
    get() = (Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())) * 0.7

private val MEM_PAGES: Double
    get() = MEM_BYTES / PAGE_BYTES

object CostModel {


    fun cost(op: LogicalOp, inputs: List<Group>): Double = when (op) {
        is ScanOp -> costScan(op)
        is JoinOp -> costGenericJoin(op, inputs)
        is HashJoinOp -> {
            if (!op.partitioned)
                costHashJoin(op, inputs)
            else costPartitionHashJoin(op, inputs)
        }

        is IndexNLJoinOp -> costIndexNL(op, inputs)
        is BlockNLJoinOp -> costBlockNL(op, inputs)
        is SortMergeJoinOp -> costSortMerge(op, inputs)
        is SortOp -> costSort(inputs.first())
        else -> Double.POSITIVE_INFINITY
    }


    private fun costScan(op: ScanOp): Double {
        val st = Catalog.stats(op.table)
        return st.filePages * C_IO + st.rows * C_CPU
    }

    private fun costGenericJoin(op: JoinOp, inputs: List<Group>): Double {
        val (l, r) = inputs
        val leftStats = Catalog.stats(l.tables.first()).col[op.leftCol]
        val rightStats = Catalog.stats(r.tables.first()).col[op.rightCol]
        val sel = selectivity(op.op, leftStats, rightStats)

        val cardL = l.bestCard
        val cardR = r.bestCard

        // штраф, если левый больше правого
        val penalty = if (cardL > cardR && cardR > 0.0) (cardL / cardR) else 1.0

        val joinCpu = (cardL * cardR * sel) * COMPARE_CPU * penalty
        return l.bestCost + r.bestCost + joinCpu
    }

    private fun costHashJoin(op: HashJoinOp, inputs: List<Group>): Double {
        val (leftGrp, rightGrp) = inputs

        val (buildGrp, probeGrp) =
            if (op.buildSideLeft) leftGrp to rightGrp else rightGrp to leftGrp

        val buildStats = Catalog.stats(buildGrp.tables.first())
        val probeStats = Catalog.stats(probeGrp.tables.first())

        val buildCard = buildGrp.bestCard
        val probeCard = probeGrp.bestCard

        val buildRowSz = buildStats.avgRowSize
        val buildBytes = buildCard * buildRowSz
        if (buildBytes > MEM_BYTES) {
            return Double.MAX_VALUE
        }

        val (buildKey, probeKey) =
            if (op.buildSideLeft) op.outerKey to op.innerKey
            else op.innerKey to op.outerKey

        val sel = selectivity(
            ConditionOperator.EQUALS,
            buildStats.col[buildKey],
            probeStats.col[probeKey]
        )

        val joinCard = buildCard * probeCard * sel

        val cpuBuild = buildCard * HASH_BUILD_CPU
        val cpuProbe = probeCard * HASH_PROBE_CPU
        val cpuMatch = joinCard * COMPARE_CPU

        val needUnmatchedBuild = op.joinType in listOf(JoinCommand.LEFT, JoinCommand.FULL) &&
                op.buildSideLeft ||
                op.joinType == JoinCommand.RIGHT && !op.buildSideLeft
        val needUnmatchedProbe = op.joinType in listOf(JoinCommand.RIGHT, JoinCommand.FULL) &&
                op.buildSideLeft ||
                op.joinType == JoinCommand.LEFT && !op.buildSideLeft

        val unmatchedBuild = if (needUnmatchedBuild) buildCard * (1 - sel) else 0.0
        val unmatchedProbe = if (needUnmatchedProbe) probeCard * (1 - sel) else 0.0
        val cpuUnmatched = (unmatchedBuild + unmatchedProbe) * COMPARE_CPU

        val totalCpu = cpuBuild + cpuProbe + cpuMatch + cpuUnmatched

        return buildGrp.bestCost +            // уже посчитанное сканирование build-таблицы
                probeGrp.bestCost +            // и probe-таблицы
                totalCpu                       // + чистый CPU хеш-джойна
    }

    private fun costPartitionHashJoin(
        op: HashJoinOp,
        inputs: List<Group>,
        depth: Int = 0,
        maxDepth: Int = 5,
    ): Double {

        /* ---------------- распаковываем входы ---------------- */
        val (leftGrp, rightGrp) = inputs
        val (buildGrp, probeGrp) =
            if (op.buildSideLeft) leftGrp to rightGrp else rightGrp to leftGrp

        /* ---------------- helper: Block NL без повторного bestCost ---------------- */
        fun blockNlFallback(): Double =
            costBlockNL(
                BlockNLJoinOp(
                    joinType = op.joinType,
                    outerKey = op.outerKey,
                    innerKey = op.innerKey,
                    outerLeft = true,               // build = left
                    condOp = ConditionOperator.EQUALS,
                ),
                listOf(buildGrp, probeGrp)          // costBlockNL **уже** включает оба bestCost
            )

        /* ---------------- базовые размеры ---------------- */
        val buildStats = Catalog.stats(buildGrp.tables.first())
        val probeStats = Catalog.stats(probeGrp.tables.first())

        val buildBytes0 = buildGrp.bestCard * buildStats.avgRowSize
        val probeBytes0 = probeGrp.bestCard * probeStats.avgRowSize
        val freeHeap = MemoryUtil.freeHeap().toDouble()

        /* ---- 1. build помещается в RAM → обычный in-mem HashJoin ---- */
        if (buildBytes0 * PartitionHashJoinIterator.MEMORY_FACTOR < freeHeap) {
            return costHashJoin(op.copy(partitioned = false), inputs)
        }

        /* ---- 2. достигли лимита рекурсии → Block NL (без +bestCost) ---- */
        if (depth >= maxDepth) return blockNlFallback()

        /* ---- 3. одна «волна» Grace-HJ + рекурсия глубже ---- */
        val numParts = ceil((2.0 * buildBytes0) / freeHeap).toInt().coerceAtLeast(2)

        /* I/O: write + read обеих таблиц */
        val ioPerWave = ((buildBytes0 + probeBytes0) / PAGE_BYTES) * 2 * C_IO

        /* CPU внутри одной партиции */
        val buildPerPart = buildGrp.bestCard / numParts
        val probePerPart = probeGrp.bestCard / numParts

        val cpuBuild = buildPerPart * HASH_BUILD_CPU
        val cpuProbe = probePerPart * HASH_PROBE_CPU

        val sel = selectivity(
            ConditionOperator.EQUALS,
            buildStats.col[op.outerKey],
            probeStats.col[op.innerKey]
        )
        val matches = buildPerPart * probePerPart * sel
        val cpuMatch = matches * COMPARE_CPU

        /* unmatched-строки для OUTER-вариантов */
        val needUnmatchedBuild =
            (op.joinType in listOf(JoinCommand.LEFT, JoinCommand.FULL) && op.buildSideLeft) ||
                    (op.joinType == JoinCommand.RIGHT && !op.buildSideLeft)

        val needUnmatchedProbe =
            (op.joinType in listOf(JoinCommand.RIGHT, JoinCommand.FULL) && op.buildSideLeft) ||
                    (op.joinType == JoinCommand.LEFT && !op.buildSideLeft)

        val cpuUnmatched =
            ((if (needUnmatchedBuild) buildPerPart * (1 - sel) else 0.0) +
                    (if (needUnmatchedProbe) probePerPart * (1 - sel) else 0.0)) * COMPARE_CPU

        val cpuPerWave = numParts * (cpuBuild + cpuProbe + cpuMatch + cpuUnmatched)

        /* ---- рекурсивная стоимость для одной «внутренней» партиции ---- */
        val fakeBuild = Group(buildGrp.tables).apply {
            bestCard = buildPerPart
            bestCost = 0.0            // сканы уже учтены верхним ioPerWave
            avgRowSize = buildGrp.avgRowSize
        }
        val fakeProbe = Group(probeGrp.tables).apply {
            bestCard = probePerPart
            bestCost = 0.0
            avgRowSize = probeGrp.avgRowSize
        }

        val deeperOne = costPartitionHashJoin(op, listOf(fakeBuild, fakeProbe), depth + 1, maxDepth)
        val deeperTotal = numParts * deeperOne

        /* ---- итоговая стоимость для текущего уровня ---- */
        return buildGrp.bestCost +              // «первый» скан build
                probeGrp.bestCost +              // «первый» скан probe
                ioPerWave + cpuPerWave + deeperTotal
    }


    private fun costIndexNL(op: IndexNLJoinOp, inputs: List<Group>): Double {
        // ───────────────────────────────────── сбор исходных данных
        val (outerGrp, innerGrp) = inputs
        val meta: IndexMeta = Catalog.index(innerGrp.tables.first(), op.innerKey)
            ?: return Double.MAX_VALUE

        val outerStats = Catalog.stats(outerGrp.tables.first())
        val innerStats = Catalog.stats(innerGrp.tables.first())

        val outerCard = outerGrp.bestCard
        val innerCard = innerGrp.bestCard

        val leftColStats = outerStats.col[op.outerKey]
        val rightColStats = innerStats.col[op.innerKey]

        val sel = selectivity(op.condOp, leftColStats, rightColStats)

        val isRange = op.condOp != ConditionOperator.EQUALS && op.condOp != ConditionOperator.NOT_EQUALS
        val perSeekSel = if (isRange)
            estimateRangeFraction(op.condOp, leftColStats, rightColStats)
        else sel

        val joinCard = outerCard * (innerCard * perSeekSel)

        val avgFanOut = (innerCard * perSeekSel) / outerCard
       // val fanOutPenalty = log2(avgFanOut.coerceAtLeast(1.0) + 1.0)  // ← unchanged

        // ─────────────────────────────────────– IO на поиск в дереве
        val seekFactor = if (op.condOp == ConditionOperator.NOT_EQUALS) 2.0 else 1.0
        val baseSeekIO = if (isRange) meta.height * C_IO_INDEX + C_IO_INDEX * avgFanOut
        else meta.height * C_IO_INDEX + C_IO_INDEX
        val indexSeekIO = outerCard * baseSeekIO * seekFactor

        // ─────────────────────────────────────– IO на чтение строк таблицы
        val tableReadIO = joinCard * C_IO  * seekFactor

        // ─────────────────────────────────────– CPU
        val joinCPU = joinCard * COMPARE_CPU * seekFactor

        // ─────────────────────────────────────– unmatched строки RIGHT/FULL
        val unmatchedInnerRows = if (op.joinType == JoinCommand.RIGHT || op.joinType == JoinCommand.FULL)
            innerCard * (1 - sel).coerceAtLeast(0.0) else 0.0

        val extraIO = unmatchedInnerRows * C_IO
        val extraCPU = unmatchedInnerRows * COMPARE_CPU

        // + полный проход по листьям индекса, чтобы найти unmatched‑inner
        val indexFullScanIO = if (op.joinType == JoinCommand.RIGHT || op.joinType == JoinCommand.FULL)
            meta.leafPages * C_IO else 0.0

        // ───────────────────────────────────── итог
        return outerGrp.bestCost +                     // стоимость скана outer
                innerGrp.bestCost +                     // стоимость скана inner
                indexSeekIO +
                tableReadIO +
                joinCPU +
                extraIO +
                extraCPU +
                indexFullScanIO
    }


    private fun estimateRangeFraction(
        op: ConditionOperator,
        left: ColStats?,
        right: ColStats?
    ): Double {
        if (left == null || right == null) return 0.5 // грубая оценка

        return when (op) {
            ConditionOperator.LESS_THAN ->
                left.tdigest.cdf(right.tdigest.quantile(0.5))

            ConditionOperator.LESS_THAN_OR_EQUALS ->
                left.tdigest.cdf(right.tdigest.quantile(0.75))

            ConditionOperator.GREATER_THAN ->
                1.0 - left.tdigest.cdf(right.tdigest.quantile(0.5))

            ConditionOperator.GREATER_THAN_OR_EQUALS ->
                1.0 - left.tdigest.cdf(right.tdigest.quantile(0.25))

            else -> 1.0
        }.coerceIn(0.01, 1.0)  // диапазон разумных значений
    }

    private fun costBlockNL(op: BlockNLJoinOp, inputs: List<Group>): Double {
        /* ---------- роли сторон ---------- */
        val (outerGrp, innerGrp) =
            if (op.outerLeft) inputs[0] to inputs[1] else inputs[1] to inputs[0]

        /* ---------- базовая статистика ---------- */
        val stO = Catalog.stats(outerGrp.tables.first())
        val stI = Catalog.stats(innerGrp.tables.first())

        val outerCard = outerGrp.bestCard            // строки в build
        val innerCard = innerGrp.bestCard            // строки в probe

        val colO = stO.col[op.outerKey]
        val colI = stI.col[op.innerKey]
        val sel = selectivity(op.condOp, colO, colI)
        val joinCard = outerCard * innerCard * sel  // выводимых строк

        /* ---------- I/O ---------- *
         * 1 полный проход по каждой таблице
         */
        val io = (stO.filePages + stI.filePages) * C_IO

        /* ---------- CPU: сравнения ключей ---------- */
        val totalComparisons = outerCard * innerCard         // проверяем ВСЕ пары
        val cpuCompare = totalComparisons * COMPARE_CPU

        /* ---------- CPU: создание output-строк для match ---------- */
        val cpuMerge = joinCard * MERGE_CPU                  // условно «merge HashMap’ов»

        /* ---------- CPU: unmatched-строки для outer-join’ов ---------- */
        val needUnmatchedOuter = (op.joinType in listOf(JoinCommand.LEFT, JoinCommand.FULL) && op.outerLeft) ||
                (op.joinType in listOf(JoinCommand.RIGHT, JoinCommand.FULL) && !op.outerLeft)
        val needUnmatchedInner = (op.joinType in listOf(JoinCommand.RIGHT, JoinCommand.FULL) && op.outerLeft) ||
                (op.joinType in listOf(JoinCommand.LEFT, JoinCommand.FULL) && !op.outerLeft)

        val unmatchedOuter = if (needUnmatchedOuter) outerCard * (1 - sel) else 0.0
        val unmatchedInner = if (needUnmatchedInner) innerCard * (1 - sel) else 0.0
        val cpuUnmatched = (unmatchedOuter + unmatchedInner) * MERGE_CPU

        /* ---------- дисбаланс сторон (кэш-эффект) ---------- */
        val outerBytes = outerCard * stO.avgRowSize
        val innerBytes = innerCard * stI.avgRowSize
        val penaltyCpu = log2((outerBytes / innerBytes).coerceAtLeast(1.0) + 1.0)

        val totalCpu = (cpuCompare + cpuMerge + cpuUnmatched) * penaltyCpu

        /* ---------- итог ---------- */
        val total = outerGrp.bestCost + innerGrp.bestCost + io + totalCpu

        println(
            "[BlockNL] sel=$sel joinCard=$joinCard " +
                    "cmp=${totalComparisons.toLong()} merge=$cpuMerge " +
                    "unmatchedO=$unmatchedOuter unmatchedI=$unmatchedInner " +
                    "io=$io cpu=$totalCpu total=$total"
        )
        return total
    }


    private fun costSortMerge(
        op: SortMergeJoinOp,
        inputs: List<Group>
    ): Double {
        val (left, right) = inputs

        /* 1.  Стоимость предварительных сортировок
               (только для EQUALS-pipeline и если входы ещё не упорядочены) */
        val needSort = (op.condOp == ConditionOperator.EQUALS)
        val costSortL = if (needSort && !op.alreadySorted) costSort(left) else 0.0
        val costSortR = if (needSort && !op.alreadySorted) costSort(right) else 0.0

        /* ---------- CPU-стоимость основной части ---------- */

        val lCard = left.bestCard
        val rCard = right.bestCard

        // «базовые» сравнения ключей
        val baseCpu = when (op.condOp) {
            ConditionOperator.EQUALS ->
                /* потоковый merge ⇒ сравниваем ≤ lCard+rCard ключей */
                (lCard + rCard) * COMPARE_CPU

            /* все остальные условные операторы выполняются
               как материализованный Вложенный Цикл над already-sorted входами */
            else -> lCard * rCard * COMPARE_CPU
        }

        /* ---------- CPU на unmatched-строки (outer-семантика) ---------- */
        val extraCpu = when (op.condOp) {
            ConditionOperator.NOT_EQUALS -> {
                // Для NOT_EQUALS план MergeJoinIterator:
                //   • INNER           → только (l != r)          (никаких unmatched)
                //   • FULL            → аналог INNER (см. итератор)
                //   • LEFT / RIGHT    → выдаём unmatched со «внешней» стороны
                when (op.joinType) {
                    JoinCommand.LEFT -> lCard * COMPARE_CPU        // (l , null)
                    JoinCommand.RIGHT -> rCard * COMPARE_CPU        // (null, r)
                    else -> 0.0                        // INNER / FULL
                }
            }

            /* Все остальные условные операторы: стандартная схема
               (для FULL unmatched с обеих сторон, для LEFT/RIGHT — с одной) */
            else -> {
                val leftNeeded = op.joinType in listOf(JoinCommand.LEFT, JoinCommand.FULL)
                val rightNeeded = op.joinType in listOf(JoinCommand.RIGHT, JoinCommand.FULL)
                (if (leftNeeded) lCard * COMPARE_CPU else 0.0) +
                        (if (rightNeeded) rCard * COMPARE_CPU else 0.0)
            }
        }

        /* ---------- итог ---------- */
        return costSortL + costSortR + baseCpu + extraCpu
    }


    private fun costSort(g: Group): Double {
        val st = Catalog.stats(g.tables.first())
        val pages = st.filePages
        val runs = ceil(pages / MEM_PAGES).coerceAtLeast(1.0)
        val passes = if (runs <= 1) 0 else ceil(ln(runs) / ln(2.0)).toInt()

        val cpu = g.bestCard * ln(g.bestCard.takeIf { it > 0.0 } ?: 1.0) * COMPARE_CPU

        val io = pages * (passes + 1) * C_IO

        return g.bestCost + cpu + io
    }

    fun selectivity(
        op: ConditionOperator,
        left: ColStats?,
        right: ColStats?
    ): Double {
        if (left == null || right == null) return 1.0 / 3

        // Учитываем NULL
        val nonNullFracL = 1.0 - (left.nullCnt / (left.ndv + left.nullCnt).coerceAtLeast(1))
        val nonNullFracR = 1.0 - (right.nullCnt / (right.ndv + right.nullCnt).coerceAtLeast(1))

        // Строковые сравнения через префиксы
        if (left.kind in setOf(CType.CHAR, CType.STRING) &&
            right.kind in setOf(CType.CHAR, CType.STRING)
        ) {

            val allKeys = (left.prefixCounts.keys + right.prefixCounts.keys).toSortedSet()
            if (allKeys.isEmpty()) return 1.0 / 3

            val lFreqs = allKeys.map { left.prefixCounts[it] ?: 0L }
            val rFreqs = allKeys.map { right.prefixCounts[it] ?: 0L }

            val lTotal = lFreqs.sum().toDouble()
            val rTotal = rFreqs.sum().toDouble()

            val intersection = lFreqs.zip(rFreqs).sumOf { (l, r) -> min(l, r) }.toDouble()
            val union = lTotal + rTotal - intersection

            val jaccard = if (union > 0) intersection / union else 1.0 / 3

            return when (op) {
                ConditionOperator.EQUALS -> jaccard
                ConditionOperator.NOT_EQUALS -> 1.0 - jaccard
                else -> jaccard // допустим через prefix approximation
            } * nonNullFracL * nonNullFracR
        }

        // Числовые типы: если есть TDigest
        if (left.tdigest.size() > 0 && right.tdigest.size() > 0) {
            val frac = when (op) {
                ConditionOperator.LESS_THAN ->
                    left.tdigest.cdf(right.tdigest.quantile(0.5))

                ConditionOperator.LESS_THAN_OR_EQUALS ->
                    left.tdigest.cdf(right.tdigest.quantile(0.75))

                ConditionOperator.GREATER_THAN ->
                    1.0 - left.tdigest.cdf(right.tdigest.quantile(0.5))

                ConditionOperator.GREATER_THAN_OR_EQUALS ->
                    1.0 - left.tdigest.cdf(right.tdigest.quantile(0.25))

                ConditionOperator.EQUALS ->
                    1.0 / max(left.ndv, right.ndv).toDouble()

                ConditionOperator.NOT_EQUALS ->
                    1.0 - 1.0 / max(left.ndv, right.ndv).toDouble()
            }

            return frac.coerceIn(0.0001, 1.0) * nonNullFracL * nonNullFracR
        }

        // Fallback: по min/max диапазону
        val lmin = left.min
        val lmax = left.max
        val rmin = right.min
        val rmax = right.max

        if (lmin > rmax || rmin > lmax) return 0.0

        val overlap = min(lmax, rmax) - max(lmin, rmin) + 1
        val union = max(lmax, rmax) - min(lmin, rmin) + 1

        val rangeSel = (overlap / union.toDouble()).coerceIn(0.0001, 1.0)

        val ndvSel = 1.0 / max(left.ndv, right.ndv).toDouble()

        val finalSel = when (op) {
            ConditionOperator.EQUALS -> ndvSel
            ConditionOperator.NOT_EQUALS -> 1.0 - ndvSel
            ConditionOperator.LESS_THAN,
            ConditionOperator.LESS_THAN_OR_EQUALS,
            ConditionOperator.GREATER_THAN,
            ConditionOperator.GREATER_THAN_OR_EQUALS -> rangeSel
        }

        println("Selectivity: ${finalSel * nonNullFracL * nonNullFracR}")
        return finalSel * nonNullFracL * nonNullFracR
    }


    fun estimateCardinality(op: LogicalOp, inputs: List<Group>): Double = when (op) {
        is ScanOp -> op.card.toDouble()
        is JoinOp -> {
            val leftStats = Catalog.stats(inputs[0].tables.first()).col[op.leftCol]
            val rightStats = Catalog.stats(inputs[1].tables.first()).col[op.rightCol]
            val sel = selectivity(op.op, leftStats, rightStats)
            inputs[0].bestCard * inputs[1].bestCard * sel
        }

        else -> inputs.first().bestCard
    }
}
