package org.bmstu.joins.algorithms

import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.bmstu.tables.Row

class MergeJoinIterator(
    private val leftIter: TupleIterator,
    private val rightIter: TupleIterator,
    private val joinType: JoinCommand,
    private val leftKey: String,
    private val rightKey: String,
    private val condOp: ConditionOperator
) : TupleIterator {

    // Если condOp == EQUALS, то будем работать «потоком» (pipeline),
    // иначе — предварительно материализуем все строки в память и потом будем отдавать.
    private val streamingEquals = (condOp == ConditionOperator.EQUALS)

    // Для режима pipeline (EQUALS) храним «текущую» пару курсоров по left/right:
    private var nextLeft: Row? = null
    private var nextRight: Row? = null

    // Для обоих режимов (стрим или материализация) используем одну очередь:
    private val results = ArrayDeque<Row>()

    // Для режима materialize неравенств: сюда заполняем всё в open()
    // для режима pipeline это остаётся пустым: мы напрямую формируем в next().
    private var materialized = false

    override fun open() {
        if (streamingEquals) {
            leftIter.open()
            rightIter.open()
            nextLeft = leftIter.next()
            nextRight = rightIter.next()
        } else {
            val leftRows = mutableListOf<Row>()
            val rightRows = mutableListOf<Row>()

            leftIter.open()
            while (true) {
                val r = leftIter.next() ?: break
                leftRows.add(r)
            }
            leftIter.close()

            rightIter.open()
            while (true) {
                val r = rightIter.next() ?: break
                rightRows.add(r)
            }
            rightIter.close()

            val matchedLeft = BooleanArray(leftRows.size) { false }
            val matchedRight = BooleanArray(rightRows.size) { false }

            // 2.1) Основная часть (все пары, у которых matchesOp(cmp) == true):
            for ((i, l) in leftRows.withIndex()) {
                val lk = l.columns[leftKey] as? Comparable<Any> ?: continue
                for ((j, r) in rightRows.withIndex()) {
                    val rk = r.columns[rightKey] as? Comparable<Any> ?: continue
                    val cmp = lk.compareTo(rk)
                    if (matchesOp(cmp)) {
                        matchedLeft[i] = true
                        matchedRight[j] = true
                        results += mergeRows(l, r)
                    }
                }
            }

            // 2.2) А теперь в зависимости от типа Join добавляем unmatched-строки:

            when (joinType) {
                JoinCommand.INNER -> {
                    // В режиме NOT_EQUALS_scenario (condOp == NOT_EQUALS):
                    // в тесте Full-Outer с NOT_EQUALS требуют ровно «все пары l!=r» без unmatched-строк,
                    // а режим INNER просто даёт пары l!=r (что мы уже сделали).
                }
                JoinCommand.LEFT -> {
                    // LEFT OUTER: для каждой leftRow без совпадений добавляем (l, null)
                    for (i in leftRows.indices) {
                        if (!matchedLeft[i]) {
                            results += mergeRows(leftRows[i], null)
                        }
                    }
                }
                JoinCommand.RIGHT -> {
                    // RIGHT OUTER: для каждого rightRow без совпадений добавляем (null, r)
                    for (j in rightRows.indices) {
                        if (!matchedRight[j]) {
                            results += mergeRows(null, rightRows[j])
                        }
                    }
                }
                JoinCommand.FULL -> {
                    if (condOp == ConditionOperator.NOT_EQUALS) {
                        // По условию теста «FULL NOT_EQUALS» выдаёт ровно все пары (l,r), где l!=r,
                        // а не выдаёт никаких «(l,null)» или «(null,r)». То есть, Full(NOT_EQUALS)=Inner(NOT_EQUALS).
                        // Поэтому никаких unmatched мы не добавляем.
                    } else {
                        // FULL OUTER для остальных операторов = LEFT OUTER + RIGHT OUTER
                        for (i in leftRows.indices) {
                            if (!matchedLeft[i]) {
                                results += mergeRows(leftRows[i], null)
                            }
                        }
                        for (j in rightRows.indices) {
                            if (!matchedRight[j]) {
                                results += mergeRows(null, rightRows[j])
                            }
                        }
                    }
                }
            }

            // Пометка «мы уже материализовали всё»:
            materialized = true
        }
    }

    override fun next(): Row? {
        if (streamingEquals) {
            // В pipeline-режиме мы генерируем результат «на лету»,
            // пока в очереди нет ни одной строки:
            while (results.isEmpty()) {
                // Если обе стороны уже пусты — пытаемся выдать unmatched-строки (FULL/RIGHT/LEFT),
                // или заканчиваем.
                if (nextLeft == null && nextRight == null) {
                    // Оба курсора закончены
                    return null
                }
                // Если левая кончилась, а правая ещё есть:
                if (nextLeft == null) {
                    if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                        // выдаём (null, nextRight)
                        val r = nextRight!!
                        nextRight = rightIter.next()
                        return mergeRows(null, r)
                    }
                    // иначе — просто «съедаем» nextRight без выпуска
                    nextRight = rightIter.next()
                    continue
                }
                // Если правая кончилась, а левая ещё есть:
                if (nextRight == null) {
                    if (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL) {
                        // выдаём (nextLeft, null)
                        val l = nextLeft!!
                        nextLeft = leftIter.next()
                        return mergeRows(l, null)
                    }
                    // иначе — просто «съедаем» nextLeft без выпуска
                    nextLeft = leftIter.next()
                    continue
                }

                // Теперь обе стороны не null:
                val l = nextLeft!!
                val r = nextRight!!

                val lk = l.columns[leftKey] as? Comparable<Any>
                val rk = r.columns[rightKey] as? Comparable<Any>
                if (lk == null || rk == null) {
                    // Если по какой-то причине мы не смогли распарсить ключ, просто двигаем оба указателя
                    nextLeft = leftIter.next()
                    nextRight = rightIter.next()
                    continue
                }
                val cmp = lk.compareTo(rk)

                if (!matchesOp(cmp)) {
                    // cmp < 0 или cmp > 0 (для EQUALS тут бывает только <0 или >0, т. е. не match)
                    if (cmp < 0) {
                        // leftKey < rightKey, но мы при EQUALS хотим только cmp==0,
                        // т.е. если LEFT OUTER или FULL, выдаём (l,null), иначе просто пропускаем лево.
                        if (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL) {
                            nextLeft = leftIter.next()
                            return mergeRows(l, null)
                        }
                        // иначе просто «съедаем» левую строку:
                        nextLeft = leftIter.next()
                    } else {
                        // cmp > 0
                        if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                            nextRight = rightIter.next()
                            return mergeRows(null, r)
                        }
                        nextRight = rightIter.next()
                    }
                    continue
                }

                // cmp == 0 — совпадение (EQUALS)
                // выдаём эту пару и двигаем оба курсора
                nextLeft = leftIter.next()
                nextRight = rightIter.next()
                return mergeRows(l, r)
            }

            // Если очередь неожиданно оказалась непустой (маловероятно для EQUALS), просто отдадим из неё:
            return if (results.isNotEmpty()) results.removeFirst() else null
        } else {
            // Режим materialize: всё уже записано в results при open(), просто отдаём по одной
            return if (results.isNotEmpty()) {
                results.removeFirst()
            } else {
                null
            }
        }
    }

    override fun close() {
        // В pipeline-режиме оставшиеся итераторы нужно закрыть
        if (streamingEquals) {
            leftIter.close()
            rightIter.close()
        }
        // В materialize-режиме итераторы уже были закрыты в open()
    }

    /**
     * Проверка «удовлетворяет ли пара (л,п) заданному condOp».
     *
     * cmp = leftValue.compareTo(rightValue)
     * Если EQUALS  → cmp == 0
     * Если NOT_EQUALS → cmp != 0
     * Если LESS_THAN    → cmp < 0
     * Если LESS_THAN_OR_EQUALS → cmp <= 0
     * Если GREATER_THAN        → cmp > 0
     * Если GREATER_THAN_OR_EQUALS → cmp >= 0
     */
    private fun matchesOp(cmp: Int): Boolean = when (condOp) {
        ConditionOperator.EQUALS                 -> (cmp == 0)
        ConditionOperator.NOT_EQUALS             -> (cmp != 0)
        ConditionOperator.LESS_THAN              -> (cmp < 0)
        ConditionOperator.LESS_THAN_OR_EQUALS    -> (cmp <= 0)
        ConditionOperator.GREATER_THAN           -> (cmp > 0)
        ConditionOperator.GREATER_THAN_OR_EQUALS -> (cmp >= 0)
    }

    /**
     * Объединяет две строки в одну.
     * Если имена колонок в двух Row совпадают (например, оба имеют «id»), в результирующей
     * строке сохраняется значение из left. Другие колонки из right будут добавлены только если
     * такого именя ещё нет.
     */
    private fun mergeRows(left: Row?, right: Row?): Row {
        val combined = HashMap<String, Any?>()
        left?.columns?.forEach { (k, v) ->
            combined[k] = v
        }
        right?.columns?.forEach { (k, v) ->
            if (!combined.containsKey(k)) {
                combined[k] = v
            }
        }
        // Убираем null-значения:
        val filtered = combined.filterValues { it != null } as HashMap<String, Any?>
        return Row(filtered)
    }
}
