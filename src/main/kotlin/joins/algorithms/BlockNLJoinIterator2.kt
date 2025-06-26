package org.bmstu.joins.algorithms

import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.bmstu.reader.TablesReader
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import org.bmstu.util.MemoryUtil
import java.io.BufferedReader
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate

class BlockNLJoinIterator(
    private val buildPath: Path,
    private val buildSchema: YSchema,
    private val probePath: Path,
    private val probeSchema: YSchema,
    private val joinType: JoinCommand,
    private val buildKey: String,
    private val probeKey: String,
    private val condOp: ConditionOperator
) : TupleIterator {

    private lateinit var probeReader: TablesReader
    private lateinit var buildReader: BufferedReader

    private val rowQueue = ArrayDeque<Row>()
    private var currentBuildBatch: List<Row> = emptyList()
    private var buildBatchMatched: BooleanArray = BooleanArray(0)

    private var probeBatches: List<List<Row>> = emptyList()
    private val matchedProbeKeys: MutableSet<Any?>? =
        if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) mutableSetOf() else null

    private var afterBuildPhase = false
    private var unmatchedProbeIterator: Iterator<Row>? = null

    private val BUILD_BATCH_SIZE = 500
    private val PROBE_BATCH_SIZE = 1000

    override fun open() {
        probeReader = TablesReader(probePath, probeSchema)
        if (MemoryUtil.canFitInRam(probeSchema.table)) {
            probeBatches = listOf(mutableListOf<Row>().apply {
                probeReader.readTbl { add(it) }
            })
        } else {
            val tmp = mutableListOf<List<Row>>()
            var batch = mutableListOf<Row>()
            probeReader.readTbl { row ->
                batch += row
                if (batch.size >= PROBE_BATCH_SIZE) {
                    tmp += batch
                    batch = mutableListOf()
                }
            }
            if (batch.isNotEmpty()) tmp += batch
            probeBatches = tmp
        }
        buildReader = Files.newBufferedReader(buildPath)
    }

    override fun next(): Row? {
        if (rowQueue.isNotEmpty()) return rowQueue.removeFirst()

        while (loadNextBuildBatch()) {
            processBuildBatch()
            if (rowQueue.isNotEmpty()) return rowQueue.removeFirst()
        }

        if (!afterBuildPhase && (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL)) {
            afterBuildPhase = true
            prepareUnmatchedProbeIterator()
        }
        if (afterBuildPhase) {
            val it = unmatchedProbeIterator ?: emptyList<Row>().iterator()
            if (it.hasNext()) return it.next()
            close()
            return null
        }

        close()
        return null
    }

    override fun close() {
        buildReader.close()
        probeReader.close()
        unmatchedProbeIterator = null
        currentBuildBatch = emptyList()
        buildBatchMatched = BooleanArray(0)
        rowQueue.clear()
        matchedProbeKeys?.clear()
    }

    private fun loadNextBuildBatch(): Boolean {
        val batch = mutableListOf<Row>()
        while (batch.size < BUILD_BATCH_SIZE) {
            val line = buildReader.readLine() ?: break
            if (line.isEmpty()) continue
            batch += parseLineToRow(line, buildSchema)
        }
        return if (batch.isNotEmpty()) {
            currentBuildBatch = batch
            buildBatchMatched = BooleanArray(batch.size) { false }
            true
        } else false
    }

    private fun processBuildBatch() {
        for ((i, bRow) in currentBuildBatch.withIndex()) {
            val bKey = bRow.columns[buildKey]
            for (probeBatch in probeBatches) {
                for (pRow in probeBatch) {
                    val pKey = pRow.columns[probeKey]
                    if (keysMatch(bKey, pKey)) {
                        buildBatchMatched[i] = true
                        matchedProbeKeys?.add(pKey)
                        rowQueue += mergeRows(bRow, pRow)
                    }
                }
            }
        }

        for ((i, bRow) in currentBuildBatch.withIndex()) {
            if (!buildBatchMatched[i] && (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL)) {
                rowQueue += mergeRows(bRow, null)
            }
        }
    }

    private fun prepareUnmatchedProbeIterator() {
        val leftovers = mutableListOf<Row>()
        for (batch in probeBatches) {
            for (row in batch) {
                val k = row.columns[probeKey]
                if (matchedProbeKeys == null || !matchedProbeKeys.contains(k)) {
                    leftovers += row
                }
            }
        }
        unmatchedProbeIterator = leftovers.iterator()
    }

    private fun parseLineToRow(line: String, schema: YSchema): Row {
        val tokens = line.split('|')
        val values = HashMap<String, Any?>()
        for ((i, col) in schema.columns.withIndex()) {
            val raw = tokens.getOrNull(i) ?: ""
            val v: Any? = when (col.type) {
                org.bmstu.reader.CType.INT     -> raw.toIntOrNull()
                org.bmstu.reader.CType.BIGINT  -> raw.toLongOrNull()
                org.bmstu.reader.CType.DECIMAL -> raw.toBigDecimalOrNull()
                org.bmstu.reader.CType.DATE    -> if (raw.isNotEmpty()) LocalDate.parse(raw) else null
                org.bmstu.reader.CType.CHAR,
                org.bmstu.reader.CType.STRING  -> raw.ifEmpty { null }
            }
            if (v != null) values[col.name] = v
        }
        return Row(values)
    }

    private fun mergeRows(left: Row?, right: Row?): Row {
        val combined = HashMap<String, Any?>()
        left?.columns?.let { combined.putAll(it) }
        right?.columns?.forEach { (k, v) ->
            if (!combined.containsKey(k)) combined[k] = v
        }
        return Row(combined)
    }

    private fun keysMatch(left: Any?, right: Any?): Boolean {
        if (left == null || right == null) return false
        @Suppress("UNCHECKED_CAST")
        return when (condOp) {
            ConditionOperator.EQUALS                 -> left == right
            ConditionOperator.NOT_EQUALS             -> left != right
            ConditionOperator.LESS_THAN              -> (left as Comparable<Any>) <  right
            ConditionOperator.LESS_THAN_OR_EQUALS    -> (left as Comparable<Any>) <= right
            ConditionOperator.GREATER_THAN           -> (left as Comparable<Any>) >  right
            ConditionOperator.GREATER_THAN_OR_EQUALS -> (left as Comparable<Any>) >= right
        }
    }
}
