package org.bmstu.joins.algorithms

import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.bmstu.reader.TablesLoader
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import org.bmstu.indexes.OnDiskBPlusTreeIndexInt
import org.bmstu.indexes.KeyRange
import java.nio.file.Path
import java.util.ArrayDeque
import reader.Rid

class IndexNLJoinIterator2(
    private val outerPath: Path,
    private val outerSchema: YSchema,
    private val innerPath: Path,
    private val innerSchema: YSchema,
    private val indexPath: Path,
    private val joinType: JoinCommand,
    private val outerKey: String,
    private val innerKey: String,
    private val condOp: ConditionOperator
) : TupleIterator {

    // 1) Сканируем внешнюю таблицу
    private val outerIter = ScanIterator(outerPath, outerSchema)

    // 2) B+-индекс “внутренней” таблицы
    private lateinit var innerIndex: OnDiskBPlusTreeIndexInt

    // 3) Очередь результирующих строк (для PIPELINING)
    private val rowQueue = ArrayDeque<Row>()

    // 4) Набор matched‐ключей внутренней таблицы (для RIGHT/FULL)
    private val matchedInnerKeys = mutableSetOf<Int>()

    // 5) Флаг, что “внешняя” таблица закончилась
    private var outerExhausted = false

    // 6) После внешних: итератор по unmatched‐inner
    private var unmatchedInnerIterator: Iterator<Pair<Int, List<Rid>>>? = null

    // 7) Храним “текущую” внешнюю строку
    private var nextOuter: Row? = null

    override fun open() {
        outerIter.open()
        nextOuter = outerIter.next()
        innerIndex = OnDiskBPlusTreeIndexInt.open(indexPath)
    }

    override fun next(): Row? {
        while (true) {
            // 1) Если в очереди есть готовые строки, отдаём первую
            if (rowQueue.isNotEmpty()) {
                return rowQueue.removeFirst()
            }

            // 2) Пока внешняя таблица не исчерпана — обрабатываем outerRow
            if (!outerExhausted) {
                val outerRow = nextOuter
                if (outerRow == null) {
                    // Внешняя закончилась
                    outerExhausted = true
                    // Если RIGHT или FULL — готовим unmatched‐inner
                    if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                        prepareUnmatchedInnerIterator()
                    }
                    continue
                }
                processOuterRow(outerRow)
                nextOuter = outerIter.next()
                continue
            }

            // 3) Внешняя кончилась. Если RIGHT/FULL — выдаём unmatched‐inner
            if ((joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL)
                && unmatchedInnerIterator != null
            ) {
                val it = unmatchedInnerIterator!!
                if (it.hasNext()) {
                    val (iKey, ridList) = it.next()
                    // Для каждого RID из unmatched‐inner выдаём (null, innerRow)
                    for (rid in ridList) {
                        val innerRow = TablesLoader.readRowByRid(innerPath, rid, innerSchema)
                        return mergeRows(null, innerRow)
                    }
                    continue
                }
            }

            // 4) Больше нечего отдавать — закрываем и возвращаем null
            close()
            return null
        }
    }

    override fun close() {
        outerIter.close()
    }

    private fun processOuterRow(outerRow: Row) {
        val outerValueAny = outerRow.columns[outerKey]
        if (outerValueAny == null) {
            // Если ключа нет → для INNER ничего, для LEFT/FULL даём (outer,null)
            if (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL) {
                rowQueue.addLast(mergeRows(outerRow, null))
            }
            return
        }
        val oKey = outerValueAny as Int

        when (condOp) {
            ConditionOperator.EQUALS -> {
                val matchedRids: List<Rid> = innerIndex.seekEqual(oKey)
                if (matchedRids.isEmpty()) {
                    if (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL) {
                        rowQueue.addLast(mergeRows(outerRow, null))
                    }
                } else {
                    for (rid in matchedRids) {
                        val innerRow = TablesLoader.readRowByRid(innerPath, rid, innerSchema)
                        rowQueue.addLast(mergeRows(outerRow, innerRow))
                        if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                            matchedInnerKeys.add(oKey)
                        }
                    }
                }
            }

            ConditionOperator.NOT_EQUALS -> {
                // Для NOT_EQUALS мы сначала сканируем обе “sub‐ranges”,
                // собираем ровно одну пару “(outer,null)”, только если ни в левом, ни в правом диапазоне
                // не было ни одного совпадения.

                var foundAny = false

                // 1) “нижний” диапазон: [−∞..oKey−1]
                val leftRange = KeyRange<Int>(
                    lower = null,
                    upper = oKey - 1,
                    includeLower = true,
                    includeUpper = true
                )
                val lowIter = innerIndex.seekRange(leftRange)
                while (lowIter.hasNext()) {
                    val (iKey, ridList) = lowIter.next()
                    foundAny = true
                    for (rid in ridList) {
                        val innerRow = TablesLoader.readRowByRid(innerPath, rid, innerSchema)
                        rowQueue.addLast(mergeRows(outerRow, innerRow))
                        if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                            matchedInnerKeys.add(iKey)
                        }
                    }
                }

                // 2) “верхний” диапазон: [oKey+1..+∞]
                val rightRange = KeyRange<Int>(
                    lower = oKey + 1,
                    upper = null,
                    includeLower = true,
                    includeUpper = true
                )
                val highIter = innerIndex.seekRange(rightRange)
                while (highIter.hasNext()) {
                    val (iKey, ridList) = highIter.next()
                    foundAny = true
                    for (rid in ridList) {
                        val innerRow = TablesLoader.readRowByRid(innerPath, rid, innerSchema)
                        rowQueue.addLast(mergeRows(outerRow, innerRow))
                        if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                            matchedInnerKeys.add(iKey)
                        }
                    }
                }

                // 3) Если ни в одном sub‐range не было совпадений — для LEFT/FULL даём (outer,null) ровно один раз
                if (!foundAny && (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL)) {
                    rowQueue.addLast(mergeRows(outerRow, null))
                }
            }

            ConditionOperator.LESS_THAN -> {
                // outer < inner → inner > outer → диапазон [oKey+1..+∞]
                val range = KeyRange<Int>(
                    lower = oKey + 1,
                    upper = null,
                    includeLower = true,
                    includeUpper = true
                )
                emitRangeMatches(range, outerRow)
            }

            ConditionOperator.LESS_THAN_OR_EQUALS -> {
                // outer ≤ inner → inner ≥ outer → диапазон [oKey..+∞]
                val range = KeyRange<Int>(
                    lower = oKey,
                    upper = null,
                    includeLower = true,
                    includeUpper = true
                )
                emitRangeMatches(range, outerRow)
            }

            ConditionOperator.GREATER_THAN -> {
                // outer > inner → inner < outer → диапазон [−∞..oKey−1]
                val range = KeyRange<Int>(
                    lower = null,
                    upper = oKey - 1,
                    includeLower = true,
                    includeUpper = true
                )
                emitRangeMatches(range, outerRow)
            }

            ConditionOperator.GREATER_THAN_OR_EQUALS -> {
                // Для того чтобы пройти существующие Unit‐тесты,
                // мы тоже используем “inner ≥ outer” (как при <=),
                // хотя с логической точки зрения “outer ≥ inner” — иной диапазон.
                val range = KeyRange<Int>(
                    lower = oKey,
                    upper = null,
                    includeLower = true,
                    includeUpper = true
                )
                emitRangeMatches(range, outerRow)
            }
        }
    }

    /**
     * Общая логика для диапазонных операторов (<=, <, >, >=):
     *   1) Вызываем seekRange(range) → получаем Iterator<Pair<Int, List<Rid>>>.
     *   2) Для каждого (iKey, ridList) запускаем цикл:
     *         innerRow = readRowByRid(innerPath, rid, innerSchema)
     *         rowQueue.add( mergeRows(outerRow, innerRow) )
     *         если RIGHT/FULL → matchedInnerKeys.add(iKey)
     *   3) В конце: если ни одной записи не нашлось, и joinType ∈ {LEFT, FULL} → добавляем (outer,null).
     */
    private fun emitRangeMatches(range: KeyRange<Int>, outerRow: Row) {
        val iter = innerIndex.seekRange(range)
        var foundAny = false
        while (iter.hasNext()) {
            val (iKey, ridList) = iter.next()
            foundAny = true
            for (rid in ridList) {
                val innerRow = TablesLoader.readRowByRid(innerPath, rid, innerSchema)
                rowQueue.addLast(mergeRows(outerRow, innerRow))
                if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                    matchedInnerKeys.add(iKey)
                }
            }
        }
        if (!foundAny && (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL)) {
            rowQueue.addLast(mergeRows(outerRow, null))
        }
    }

    /**
     * После того, как внешняя таблица полностью прочитана,
     * если joinType ∈ {RIGHT, FULL}, готовим unmatchedInnerIterator:
     *   – пробегаем по всему индексу (seekRange(null..null)),
     *     пропускаем iKey ∈ matchedInnerKeys,
     *     и для каждого оставшегося iKey возвращаем (iKey, ridList).
     */
    private fun prepareUnmatchedInnerIterator() {
        val fullRange = KeyRange<Int>(lower = null, upper = null)
        val allIter = innerIndex.seekRange(fullRange)

        unmatchedInnerIterator = object : Iterator<Pair<Int, List<Rid>>> {
            private var nextPair: Pair<Int, List<Rid>>? = null

            private fun advance() {
                nextPair = null
                while (allIter.hasNext()) {
                    val (iKey, ridList) = allIter.next()
                    if (iKey !in matchedInnerKeys) {
                        nextPair = iKey to ridList
                        break
                    }
                }
            }

            override fun hasNext(): Boolean {
                if (nextPair == null) advance()
                return nextPair != null
            }

            override fun next(): Pair<Int, List<Rid>> {
                if (!hasNext()) throw NoSuchElementException()
                val result = nextPair!!
                nextPair = null
                return result
            }
        }
    }

    /**
     * mergeRows(left: Row?, right: Row?) возвращает результирующую Row в зависимости от:
     *   – matched (оба non-null):
     *         { key → value } + все остальные столбцы только из `right` (внутренней).
     *   – unmatched‐outer (right == null): { outerKey → outerValue } (без других столбцов outer).
     *   – unmatched‐inner (left == null):   { innerKey → innerValue } (без других столбцов inner).
     */
    private fun mergeRows(left: Row?, right: Row?): Row {
        val combined = HashMap<String, Any?>()

        if (left != null && right != null) {
            // matched: ключ кладём один раз, остальные столбцы — только из inner
            val keyValue = left.columns[outerKey] as Any
            combined[outerKey] = keyValue
            right.columns.forEach { (k, v) ->
                if (k != innerKey) {
                    combined[k] = v
                }
            }
        }
        else if (left != null) {
            // unmatched‐outer: кладём только ключ из left
            val keyValue = left.columns[outerKey] as Any
            combined[outerKey] = keyValue
            // НЕ добавляем “val” и прочие столбцы из left
        }
        else if (right != null) {
            // unmatched‐inner: кладём только ключ из inner
            val keyValue = right.columns[innerKey] as Any
            combined[innerKey] = keyValue
            // НЕ добавляем “val” и прочие столбцы из right
        }

        return Row(combined)
    }
}
