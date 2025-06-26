package org.bmstu.joins.algorithms

import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.Objects
import kotlin.math.ceil
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import org.bmstu.util.MemoryUtil


class PartitionHashJoinIterator(
    private val buildPath: Path,
    private val buildSchema: YSchema,
    private val probePath: Path,
    private val probeSchema: YSchema,
    private val joinType: JoinCommand,
    private val buildKey: String,
    private val probeKey: String,
    private val condOp: ConditionOperator,
    private val hashSalt: Int = 0,
    private val currentDepth: Int = 0
) : TupleIterator {

    companion object {
        private const val MAX_RECURSION_DEPTH = 5
        const val MEMORY_FACTOR = 6
    }

    // Для разбивки на партиции
    private lateinit var tmpDir: Path
    private val buildPartitions = mutableListOf<Path>()
    private val probePartitions = mutableListOf<Path>()
    private var numPartitions: Int = 0

    // Текущий индекс партиции [0..numPartitions-1]
    private var currentPartitionIndex: Int = 0

    // Текущий внутренний итератор по партиции
    private var currentPartitionIter: TupleIterator? = null

    // Флаг: партиции уже созданы (open() завершился)
    private var partitionsCreated = false

    override fun open() {
        require(condOp == ConditionOperator.EQUALS) {
            "PartitionHashJoinIterator поддерживает только EQUALS"
        }

        val buildSize = Files.size(buildPath)
        val freeHeap = MemoryUtil.freeHeap()

        // 1) Если build помещается полностью → In-Memory Hash Join одной итерацией
        if (buildSize * MEMORY_FACTOR < (MemoryUtil.freeHeap() * 0.4).toLong()) {
            val inMemIter = InMemoryHashJoinPartition(
                buildPath, buildSchema,
                probePath, probeSchema,
                joinType, buildKey, probeKey
            )
            inMemIter.open()
            currentPartitionIter = inMemIter
            partitionsCreated = true
            return
        }

        // 2) Если достигли максимальной глубины → блочный Nested-Loop Join одной итерацией
        if (currentDepth >= MAX_RECURSION_DEPTH) {
            val blockIter = BlockNLJoinPartition(
                buildPath, buildSchema,
                probePath, probeSchema,
                joinType, buildKey, probeKey
            )
            blockIter.open()
            currentPartitionIter = blockIter
            partitionsCreated = true
            return
        }

        // 3) Иначе дробим build & probe на numPartitions
        numPartitions = ceil((2.0 * buildSize) / freeHeap).toInt().coerceAtLeast(2)
        tmpDir = Files.createTempDirectory("partition_hash_join_")
        repeat(numPartitions) { idx ->
            val bfile = tmpDir.resolve("build_part_$idx.tbl")
            val pfile = tmpDir.resolve("probe_part_$idx.tbl")
            Files.createFile(bfile)
            Files.createFile(pfile)
            buildPartitions.add(bfile)
            probePartitions.add(pfile)
        }

        splitBuildIntoPartitions()
        splitProbeIntoPartitions()

        currentPartitionIndex = 0
        partitionsCreated = true
    }

    override fun next(): Row? {
        if (!partitionsCreated) {
            throw IllegalStateException("Нужно вызвать open() перед next()")
        }

        while (true) {
            // Если внутренний итератор не готов → создать его для следующей непустой партиции
            if (currentPartitionIter == null) {
                // Все партиции обработаны
                if (currentPartitionIndex >= numPartitions) return null

                // Создаем итератор для этой партиции
                currentPartitionIter = makeIteratorForPartition(currentPartitionIndex)
                currentPartitionIter!!.open()
            }

            // Пытаемся взять строку из текущего итератора
            val row = currentPartitionIter!!.next()
            if (row != null) {
                return row
            }

            // Если итератор вернул null → партиция закончена
            currentPartitionIter!!.close()
            try {
                Files.deleteIfExists(buildPartitions[currentPartitionIndex])
                Files.deleteIfExists(probePartitions[currentPartitionIndex])
            } catch (_: Exception) { }

            currentPartitionIndex++
            currentPartitionIter = null
        }
    }

    override fun close() {
        currentPartitionIter?.close()
        // Удалить оставшиеся файлы партиций
        buildPartitions.forEach { Files.deleteIfExists(it) }
        probePartitions.forEach { Files.deleteIfExists(it) }
        try { Files.deleteIfExists(tmpDir) } catch (_: Exception) { }
    }

    private fun splitBuildIntoPartitions() {
        val writers = buildPartitions.map { path ->
            Files.newBufferedWriter(path, StandardOpenOption.WRITE)
        }
        val bIter = ScanIterator(buildPath, buildSchema)
        bIter.open()
        while (true) {
            val brow = bIter.next() ?: break
            val key = brow.columns[buildKey] ?: continue
            val idx = hashPartition(key)
            writers[idx].write(serializeRow(brow, buildSchema))
            writers[idx].newLine()
        }
        bIter.close()
        writers.forEach { it.close() }
    }

    private fun splitProbeIntoPartitions() {
        val writers = probePartitions.map { path ->
            Files.newBufferedWriter(path, StandardOpenOption.WRITE)
        }
        val pIter = ScanIterator(probePath, probeSchema)
        pIter.open()
        while (true) {
            val prow = pIter.next() ?: break
            val key = prow.columns[probeKey] ?: continue
            val idx = hashPartition(key)
            writers[idx].write(serializeRow(prow, probeSchema))
            writers[idx].newLine()
        }
        pIter.close()
        writers.forEach { it.close() }
    }

    private fun makeIteratorForPartition(i: Int): TupleIterator {
        val bPart = buildPartitions[i]
        val pPart = probePartitions[i]
        val sizeB = Files.size(bPart)
        val freeHeapNow = MemoryUtil.freeHeap()

        return when {
            // Помещается в память → InMemory Hash-Join партиции
            freeHeapNow > sizeB * MEMORY_FACTOR -> {
                InMemoryHashJoinPartition(
                    bPart, buildSchema,
                    pPart, probeSchema,
                    joinType, buildKey, probeKey
                )
            }
            // Достигли глубины → блочный NL-Join партиции
            currentDepth + 1 >= MAX_RECURSION_DEPTH -> {
                BlockNLJoinPartition(
                    bPart, buildSchema,
                    pPart, probeSchema,
                    joinType, buildKey, probeKey
                )
            }
            // Иначе рекурсивно дробим дальше
            else -> {
                PartitionHashJoinIterator(
                    buildPath    = bPart,
                    buildSchema  = buildSchema,
                    probePath    = pPart,
                    probeSchema  = probeSchema,
                    joinType     = joinType,
                    buildKey     = buildKey,
                    probeKey     = probeKey,
                    condOp       = condOp,
                    hashSalt     = this.hashSalt + 1,
                    currentDepth = this.currentDepth + 1
                )
            }
        }
    }

    private fun hashPartition(key: Any): Int {
        val hval = Objects.hash(key, hashSalt)
        val positive = hval and Int.MAX_VALUE
        return positive % numPartitions
    }

    private fun serializeRow(row: Row, schema: YSchema): String {
        return schema.columns.joinToString("|") { col ->
            val v = row.columns[col.name]
            v?.toString() ?: ""
        }
    }

    private class InMemoryHashJoinPartition(
        private val leftPath: Path,
        private val leftSchema: YSchema,
        private val rightPath: Path,
        private val rightSchema: YSchema,
        private val joinType: JoinCommand,
        private val leftKey: String,
        private val rightKey: String
    ) : TupleIterator {

        private val hashTable = mutableMapOf<Any, MutableList<Row>>()
        private val matchedLeft = mutableSetOf<Row>()

        private var probeIter: TupleIterator? = null
        private var currentMatches: Iterator<Row>? = null
        private var currentProbeRow: Row? = null
        private var probeExhausted = false

        private val leftQueue = ArrayDeque<Row>()
        private val rightQueue = ArrayDeque<Row>()

        override fun open() {
            val lIter = ScanIterator(leftPath, leftSchema)
            lIter.open()
            while (true) {
                val lrow = lIter.next() ?: break
                val key = lrow.columns[leftKey] ?: continue
                hashTable.computeIfAbsent(key) { mutableListOf() }.add(lrow)
            }
            lIter.close()

            probeIter = ScanIterator(rightPath, rightSchema)
            probeIter!!.open()
        }

        override fun next(): Row? {
            if (rightQueue.isNotEmpty()) {
                return rightQueue.removeFirst()
            }
            if (!probeExhausted) {
                val pIter = probeIter!!
                while (true) {
                    currentMatches?.let { matches ->
                        if (matches.hasNext()) {
                            val lrow = matches.next().also { matchedLeft += it }
                            val prow = currentProbeRow!!
                            return mergeRows(lrow, prow)
                        } else {
                            currentMatches = null
                        }
                    }
                    val prow = pIter.next()
                    if (prow == null) {
                        probeExhausted = true
                        break
                    }
                    currentProbeRow = prow
                    val key = prow.columns[rightKey] ?: continue
                    val bucket = hashTable[key] ?: emptyList()
                    if (bucket.isNotEmpty()) {
                        currentMatches = bucket.iterator()
                        continue
                    }
                    if (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL) {
                        return mergeRows(null, prow)
                    }
                }
            }
            if (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL) {
                for (keyBucket in hashTable) {
                    for (lrow in keyBucket.value) {
                        if (lrow !in matchedLeft) {
                            matchedLeft += lrow
                            return mergeRows(lrow, null)
                        }
                    }
                }
            }
            return null
        }

        override fun close() {
            probeIter?.close()
            hashTable.clear()
            matchedLeft.clear()
            currentMatches = null
            currentProbeRow = null
            leftQueue.clear()
            rightQueue.clear()
        }

        private fun mergeRows(left: Row?, right: Row?): Row {
            val combined = HashMap<String, Any?>()
            left?.columns?.forEach  { (k, v) -> if (v != null) combined[k] = v }
            right?.columns?.forEach { (k, v) -> if (v != null) combined[k] = v }
            @Suppress("UNCHECKED_CAST")
            return Row(combined.filterValues { it != null }
                    as HashMap<String, Any> as HashMap<String, Any?>)
        }
    }

    private class BlockNLJoinPartition(
        private val leftPath: Path,
        private val leftSchema: YSchema,
        private val rightPath: Path,
        private val rightSchema: YSchema,
        private val joinType: JoinCommand,
        private val leftKey: String,
        private val rightKey: String
    ) : TupleIterator {

        private val leftRows = mutableListOf<Row>()
        private val matchedLeft = mutableSetOf<Row>()

        private var rightIter: TupleIterator? = null
        private val unmatchedRightQueue = ArrayDeque<Row>()

        override fun open() {
            val lIter = ScanIterator(leftPath, leftSchema)
            lIter.open()
            while (true) {
                val lrow = lIter.next() ?: break
                leftRows.add(lrow)
            }
            lIter.close()

            rightIter = ScanIterator(rightPath, rightSchema)
            rightIter!!.open()
        }

        override fun next(): Row? {
            if (unmatchedRightQueue.isNotEmpty()) {
                return unmatchedRightQueue.removeFirst()
            }
            val rIter = rightIter!!
            while (true) {
                val prow = rIter.next() ?: break
                val keyR = prow.columns[rightKey]
                var found = false
                for (lrow in leftRows) {
                    val keyL = lrow.columns[leftKey]
                    if (keyL == keyR) {
                        matchedLeft += lrow
                        return mergeRows(lrow, prow)
                    }
                }
                if (!found && (joinType == JoinCommand.RIGHT || joinType == JoinCommand.FULL)) {
                    return mergeRows(null, prow)
                }
            }
            if (joinType == JoinCommand.LEFT || joinType == JoinCommand.FULL) {
                for (lrow in leftRows) {
                    if (lrow !in matchedLeft) {
                        matchedLeft += lrow
                        return mergeRows(lrow, null)
                    }
                }
            }
            return null
        }

        override fun close() {
            rightIter?.close()
            leftRows.clear()
            matchedLeft.clear()
            unmatchedRightQueue.clear()
        }

        private fun mergeRows(left: Row?, right: Row?): Row {
            val combined = HashMap<String, Any?>()
            left?.columns?.forEach  { (k, v) -> if (v != null) combined[k] = v }
            right?.columns?.forEach { (k, v) -> if (v != null) combined[k] = v }
            @Suppress("UNCHECKED_CAST")
            return Row(combined.filterValues { it != null }
                    as HashMap<String, Any> as HashMap<String, Any?>)
        }
    }
}
