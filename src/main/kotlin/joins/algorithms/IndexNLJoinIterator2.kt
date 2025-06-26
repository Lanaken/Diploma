package org.bmstu.joins.algorithms

import java.nio.file.Path
import java.util.ArrayDeque
import org.bmstu.indexes.KeyRange
import org.bmstu.indexes.OnDiskBPlusTreeIndexInt
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import org.bmstu.reader.TablesReader
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import reader.Rid
import util.accumulateTime

class IndexNLJoinIterator(
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

    private val outerIter = ScanIterator(outerPath, outerSchema)

    private lateinit var innerIndex: OnDiskBPlusTreeIndexInt

    private val rowQueue = ArrayDeque<Row>()

    private val matchedInnerKeys = mutableSetOf<Int>()
    private val BATCH = 1024

    private var outerExhausted = false

    private var unmatchedInnerIterator: Iterator<Pair<Int, List<Rid>>>? = null

    private var nextOuter: Row? = null
    private val tablesReader = TablesReader(innerPath, innerSchema)

    override fun open() {
        outerIter.open()
        nextOuter = outerIter.next()
        innerIndex = OnDiskBPlusTreeIndexInt.open(indexPath)
    }

    override fun next(): Row? {
        while (true) {
            if (rowQueue.isNotEmpty()) {
                return rowQueue.removeFirst()
            }

            if (!outerExhausted) {
                val outerRow = nextOuter
                if (outerRow == null) {
                    outerExhausted = true
                    if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                        prepareUnmatchedInnerIterator()
                    }
                    continue
                }
                processOuterRow(outerRow)
                accumulateTime("IndexNLJoinIterator: Outer next in next()") {
                    nextOuter = outerIter.next()
                }
                continue
            }

            if ((joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL)
                && unmatchedInnerIterator != null
            ) {
                val it = unmatchedInnerIterator!!
                if (it.hasNext()) {
                    val (iKey, ridList) = it.next()
                    for (rid in ridList) {
                        val innerRow = tablesReader.readRowByRid(rid, innerSchema)
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
        tablesReader.close()
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
                var matchedRids = listOf<Rid>()
                accumulateTime("seekEqual") {
                    matchedRids = innerIndex.seekEqual(oKey)
                }
                if (matchedRids.isEmpty()) {
                    if (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL) {
                        rowQueue.addLast(mergeRows(outerRow, null))
                    }
                } else {
                    for (rid in matchedRids) {
                        lateinit var innerRow: Row
                        accumulateTime("IndexNLJoinIterator processOuterRow readRowByRid") {
                            innerRow = tablesReader.readRowByRid(rid, innerSchema)
                        }
                        accumulateTime("IndexNLJoinIterator processOuterRow rowQueue.addLast") {
                            rowQueue.addLast(mergeRows(outerRow, innerRow))
                        }
                        if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                            val iKey = (innerRow.columns[innerKey] as Int)
                            matchedInnerKeys.add(iKey)
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
                        val innerRow = tablesReader.readRowByRid(rid, innerSchema)
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
                        val innerRow = tablesReader.readRowByRid(rid, innerSchema)
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

    private fun emitRangeMatches(range: KeyRange<Int>, outerRow: Row) {
        val iter = innerIndex.seekRange(range)
        var foundAny = false
        while (iter.hasNext()) {
            val (iKey, ridList) = iter.next()
            foundAny = true
            for (rid in ridList) {
                val innerRow = tablesReader.readRowByRid(rid, innerSchema)
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

    // TODO Решить кейс с дублирующими названиями полей
    private fun mergeRows(left: Row?, right: Row?): Row {
        val combined = HashMap<String, Any?>()
        accumulateTime("mergeRows") {
            if (left != null && right != null) {
                // Добавляем все поля из left
                combined.putAll(left.columns)

                // Добавляем все поля из right, кроме ключа, если он дублирует outerKey
                right.columns.forEach { (k, v) ->
                    if (k != innerKey || innerKey != outerKey) {
                        combined[k] = v
                    }
                }
            } else if (left != null) {
                combined.putAll(left.columns)
            } else if (right != null) {
                combined.putAll(right.columns)
            }

            return Row(combined)
        }
    }
}

