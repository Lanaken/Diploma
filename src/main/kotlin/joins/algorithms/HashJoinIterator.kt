package org.bmstu.joins.algorithms

import org.bmstu.joins.JoinCommand
import org.bmstu.tables.Row

class HashJoinIterator(
    private val buildIter: TupleIterator,
    private val probeIter: TupleIterator,
    private val buildOnLeft: Boolean,
    private val buildKey: String,
    private val probeKey: String,
    private val joinType: JoinCommand = JoinCommand.INNER
) : TupleIterator {

    /* ---------- build-side ---------- */
    private val hashTable = mutableMapOf<Any, MutableList<Row>>()
    private val matchedBuild = mutableSetOf<Row>()
    private lateinit var unmatchedIter: Iterator<Row>

    /* ---------- probe-side ---------- */
    private var probeExhausted = false
    private var currentMatches: Iterator<Row>? = null
    private var currentProbeRow: Row? = null

    /* ---------- flags ---------- */
    private var outerIsProbe = false
    private var outerIsBuild = false
    private var opened       = false

    /* ---------- helpers ---------- */
    companion object { private val NULL_ROW = Row(hashMapOf()) }

    /* ================================================================= */

    override fun open() {
        require(!opened) { "HashJoinIterator.open() already called" }

        /* --- заполняем hashTable из build --- */
        buildIter.open()
        while (true) {
            val row = buildIter.next() ?: break
            val key = row.columns[buildKey]
                ?: error("Ключ '$buildKey' отсутствует в build-row: $row")
            hashTable.computeIfAbsent(key) { mutableListOf() }.add(row)
        }
        buildIter.close()

        /* --- outer-флаги вычисляем один раз --- */
        outerIsProbe = when (joinType) {
            JoinCommand.LEFT  -> !buildOnLeft
            JoinCommand.RIGHT ->  buildOnLeft
            JoinCommand.FULL  ->  true
            else              ->  false
        }
        outerIsBuild = when (joinType) {
            JoinCommand.LEFT  ->  buildOnLeft
            JoinCommand.RIGHT -> !buildOnLeft
            JoinCommand.FULL  ->  true
            else              ->  false
        }

        unmatchedIter = hashTable.values.flatten().iterator()

        probeIter.open()
        opened = true
    }

    override fun next(): Row? {
        /* ---------- probe-сторона ---------- */
        if (!probeExhausted) {
            while (true) {
                currentMatches?.let { bucket ->
                    if (bucket.hasNext()) {
                        val buildRow = bucket.next().also { matchedBuild += it }
                        val probeRow = currentProbeRow!!
                        return if (buildOnLeft) mergeRows(buildRow, probeRow)
                        else                mergeRows(probeRow, buildRow)
                    } else currentMatches = null
                }

                val probeRow = probeIter.next()
                if (probeRow == null) { probeExhausted = true; break }

                val key = probeRow.columns[probeKey]
                    ?: error("Ключ '$probeKey' отсутствует в probe-row: $probeRow")

                val bucket = hashTable[key] ?: emptyList()
                if (bucket.isNotEmpty()) {
                    currentProbeRow = probeRow
                    currentMatches  = bucket.iterator()
                    continue
                }

                if (outerIsProbe) {
                    return if (buildOnLeft) mergeRows(NULL_ROW, probeRow)
                    else               mergeRows(probeRow, NULL_ROW)
                }
            }
        }

        /* ---------- unmatched build-строки ---------- */
        if (outerIsBuild) {
            while (unmatchedIter.hasNext()) {
                val b = unmatchedIter.next()
                if (b !in matchedBuild) {
                    matchedBuild += b
                    return if (buildOnLeft) mergeRows(b, NULL_ROW)
                    else               mergeRows(NULL_ROW, b)
                }
            }
        }
        return null
    }

    override fun close() {
        probeIter.close()
        hashTable.clear()
        matchedBuild.clear()
    }

    /* ---------- merge helper ---------- */
    private fun mergeRows(left: Row?, right: Row?): Row =
        Row(HashMap<String, Any?>().apply {
            left?.columns?.forEach  { (k, v) -> if (v != null) this[k] = v }
            right?.columns?.forEach { (k, v) -> if (v != null) this.putIfAbsent(k, v) }
        })
}
