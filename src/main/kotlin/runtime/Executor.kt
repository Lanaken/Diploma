package org.bmstu.execution

import logical.Expression
import operators.*
import org.bmstu.joins.algorithms.*
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import kotlin.math.ceil
import org.bmstu.joins.ConditionOperator
import statistic.Catalog
import statistic.Stats


class Executor(
    private val tablePaths: Map<String, Path>,
    private val schemas: Map<String, YSchema>
) {
    private val tempWorkDir: Path = Files.createTempDirectory("executor_tmp_")

    init {
        tempWorkDir.toFile().deleteOnExit()
    }

    fun executePhysicalPlanToFile(expr: Expression, outputPath: Path) {
        val (resultTemp, _) = materializeExpression(expr)
        Files.createDirectories(outputPath.parent)
        Files.copy(resultTemp, outputPath, StandardCopyOption.REPLACE_EXISTING)
    }


    private fun materializeExpression(expr: Expression): Pair<Path, YSchema> {
        println("=== Executor: обрабатываем Expression ─────────────────────────────────")
        println("Operator: ${expr.op}")
        println("Children групп: [${expr.children.joinToString { it.tables.toString() }}]")
        println("───────────────────────────────────────────────────────────────────────────")

        return when (val op = expr.op) {

            is ScanOp -> {
                println("  → ScanOp(${op.table}): возвращаем исходный файл")
                val original = tablePaths[op.table]
                    ?: throw IllegalStateException("Не найден путь к таблице '${op.table}' при выполнении ScanOp")
                println("    путь к файлу: $original")
                val schema = schemas[op.table]
                    ?: throw IllegalStateException("Нет YSchema для таблицы '${op.table}'")
                Pair(original, schema)
            }

            is SortOp -> {
                println("  → SortOp(ключ='${op.key}')")
                val childGroup = expr.children.getOrNull(0)
                    ?: throw IllegalStateException("Не удалось получить группу-ребёнка у SortOp")
                val childExpr = childGroup.bestExpression
                    ?: throw IllegalStateException("У группы для SortOp отсутствует bestExpression")
                println("    сортируем результат этого Expr: ${childExpr.op}")

                val (childFile, childSchema) = materializeExpression(childExpr)
                println("    файл до сортировки: $childFile")

                val outFile = Files.createTempFile(tempWorkDir, "sort_${op.key}_", ".tbl")
                println("    создаём временный файл для отсортированного вывода: $outFile")

                println("    запускаем simpleSortByColumn(input=$childFile, column='${op.key}', output=$outFile)")
                simpleSortByColumn(
                    input = childFile,
                    schema = childSchema,
                    columnName = op.key,
                    output = outFile
                )

                println("    → sorted файл: $outFile")
                Pair(outFile, childSchema)
            }

            is HashJoinOp -> {
                println("  → HashJoinOp(joinType=${op.joinType}, outerKey='${op.outerKey}', innerKey='${op.innerKey}', buildSideLeft=${op.buildSideLeft}, partitioned=${op.partitioned})")

                val leftGroup = expr.children.getOrNull(0)
                    ?: throw IllegalStateException("Не удалось получить левую группу у HashJoinOp")
                val leftExpr = leftGroup.bestExpression
                    ?: throw IllegalStateException("У левой группы для HashJoinOp отсутствует bestExpression")
                println("    материализуем левую ветку (${leftExpr.op})")
                val (leftFile, leftSchema) = materializeExpression(leftExpr)
                println("    → внешний файл для build (левая ветка): $leftFile")

                val rightGroup = expr.children.getOrNull(1)
                    ?: throw IllegalStateException("Не удалось получить правую группу у HashJoinOp")
                val rightExpr = rightGroup.bestExpression
                    ?: throw IllegalStateException("У правой группы для HashJoinOp отсутствует bestExpression")
                println("    материализуем правую ветку (${rightExpr.op})")
                val (rightFile, rightSchema) = materializeExpression(rightExpr)
                println("    → внешний файл для probe (правая ветка): $rightFile")

                val buildFile: Path
                val buildSchema: YSchema
                val probeFile: Path
                val probeSchema: YSchema
                val buildKey: String
                val probeKey: String

                if (op.buildSideLeft) {
                    println("    buildSideLeft=true → buildFile = leftFile, probeFile = rightFile")
                    buildFile = leftFile
                    buildSchema = leftSchema
                    probeFile = rightFile
                    probeSchema = rightSchema
                    buildKey = op.outerKey
                    probeKey = op.innerKey
                } else {
                    println("    buildSideLeft=false → buildFile = rightFile, probeFile = leftFile")
                    buildFile = rightFile
                    buildSchema = rightSchema
                    probeFile = leftFile
                    probeSchema = leftSchema
                    buildKey = op.innerKey
                    probeKey = op.outerKey
                }

                println("    buildFile    = $buildFile")
                println("    buildSchema  (число полей) = ${buildSchema.columns.size}")
                println("    probeFile    = $probeFile")
                println("    probeSchema  (число полей) = ${probeSchema.columns.size}")
                println("    buildKey     = '$buildKey'")
                println("    probeKey     = '$probeKey'")

                val outFile = Files.createTempFile(tempWorkDir, "hashjoin_", ".tbl")
                println("    создаём временный файл-вывода для HashJoin: $outFile")

                val buildIter = ScanIterator(buildFile, buildSchema)
                val probeIter = ScanIterator(probeFile, probeSchema)
                val iterator: TupleIterator = if (op.partitioned) {
                    println("    → Partitioned = true → запускаем PartitionHashJoinIterator")
                    PartitionHashJoinIterator(
                        buildPath = buildFile,
                        buildSchema = buildSchema,
                        probePath = probeFile,
                        probeSchema = probeSchema,
                        joinType = op.joinType,
                        buildKey = buildKey,
                        probeKey = probeKey,
                        condOp = ConditionOperator.EQUALS
                    )
                } else {
                    println("    → Partitioned = false → запускаем HashJoinIterator (в память)")
                    HashJoinIterator(
                        buildIter = buildIter,
                        probeIter = probeIter,
                        buildOnLeft = op.buildSideLeft,
                        buildKey = buildKey,
                        probeKey = probeKey,
                        joinType = op.joinType
                    )
                }

                writeIteratorToFile(iterator, outFile, buildSchema, probeSchema)
                println("    → HashJoin результат записан в $outFile")

                val joinedColumns = buildSchema.columns + probeSchema.columns
                val joinedSchema = YSchema(table = "join", columns = joinedColumns)
                Pair(outFile, joinedSchema)
            }

            is BlockNLJoinOp -> {
                println("  → BlockNLJoinOp(joinType=${op.joinType}, outerKey='${op.outerKey}', innerKey='${op.innerKey}', outerLeft=${op.outerLeft}, condOp=${op.condOp})")

                val leftGroup = expr.children.getOrNull(0)
                    ?: throw IllegalStateException("Не удалось получить левую группу у BlockNLJoinOp")
                val leftExpr = leftGroup.bestExpression
                    ?: throw IllegalStateException("У левой группы для BlockNLJoinOp отсутствует bestExpression")
                println("    материализуем левую ветку (${leftExpr.op})")
                val (leftFile, leftSchema) = materializeExpression(leftExpr)
                println("    → левый файл (build): $leftFile")

                val rightGroup = expr.children.getOrNull(1)
                    ?: throw IllegalStateException("Не удалось получить правую группу у BlockNLJoinOp")
                val rightExpr = rightGroup.bestExpression
                    ?: throw IllegalStateException("У правой группы для BlockNLJoinOp отсутствует bestExpression")
                println("    материализуем правую ветку (${rightExpr.op})")
                val (rightFile, rightSchema) = materializeExpression(rightExpr)
                println("    → правый файл (probe): $rightFile")

                // (в) Создаём временный файл-вывод
                val outFile = Files.createTempFile(tempWorkDir, "blocknljoin_", ".tbl")
                println("    создаём временный файл-вывода: $outFile")

                // (г) Запускаем BlockNLJoinIterator
                println("    запускаем BlockNLJoinIterator...")
                val iterator = BlockNLJoinIterator(
                    buildPath = leftFile,
                    buildSchema = leftSchema,
                    probePath = rightFile,
                    probeSchema = rightSchema,
                    joinType = op.joinType,
                    buildKey = op.outerKey,
                    probeKey = op.innerKey,
                    condOp = op.condOp
                )
                writeIteratorToFile(iterator, outFile, leftSchema, rightSchema)
                println("    → BlockNLJoin результат записан в $outFile")

                val joinedColumns = leftSchema.columns + rightSchema.columns
                val joinedSchema = YSchema(table = "join", columns = joinedColumns)

                Pair(outFile, joinedSchema)
            }

            is IndexNLJoinOp -> {
                println("  → IndexNLJoinOp(joinType=${op.joinType}, outerKey='${op.outerKey}', innerKey='${op.innerKey}', indexPath=${op.indexPath}, condOp=${op.condOp})")

                val outerGroup = expr.children.getOrNull(0)
                    ?: throw IllegalStateException("Не удалось получить внешнюю группу у IndexNLJoinOp")
                val outerExpr = outerGroup.bestExpression
                    ?: throw IllegalStateException("У внешней группы для IndexNLJoinOp отсутствует bestExpression")
                println("    материализуем внешнюю ветку (${outerExpr.op})")
                val (outerFile, outerSchema) = materializeExpression(outerExpr)
                println("    → внешний файл: $outerFile")

                val innerGroup = expr.children.getOrNull(1)
                    ?: throw IllegalStateException("Не удалось получить внутреннюю группу у IndexNLJoinOp")
                val innerExpr = innerGroup.bestExpression
                    ?: throw IllegalStateException("У внутренней группы для IndexNLJoinOp отсутствует bestExpression")
                println("    материализуем внутреннюю ветку (${innerExpr.op})")
                val (innerFile, innerSchema) = materializeExpression(innerExpr)
                println("    → внутренний файл: $innerFile")

                val outFile = Files.createTempFile(tempWorkDir, "indexnljoin_", ".tbl")
                println("    создаём временный файл-вывода: $outFile")

                println("    запускаем IndexNLJoinIterator...")
                val iterator = IndexNLJoinIterator(
                    outerPath = outerFile,
                    outerSchema = outerSchema,
                    innerPath = innerFile,
                    innerSchema = innerSchema,
                    indexPath = op.indexPath,
                    joinType = op.joinType,
                    outerKey = op.outerKey,
                    innerKey = op.innerKey,
                    condOp = op.condOp
                )
                writeIteratorToFile(iterator, outFile, outerSchema, innerSchema)
                println("    → IndexNLJoin результат записан в $outFile")

                val joinedColumns = outerSchema.columns + innerSchema.columns
                val joinedSchema = YSchema(table = "join", columns = joinedColumns)

                Pair(outFile, joinedSchema)
            }

            is SortMergeJoinOp -> {
                println("  → SortMergeJoinOp(joinType=${op.joinType}, outerKey='${op.outerKey}', innerKey='${op.innerKey}', alreadySorted=${op.alreadySorted}, condOp=${op.condOp})")

                // (а) Материализуем левую ветку «сырую»
                val leftGroup = expr.children.getOrNull(0)
                    ?: throw IllegalStateException("Не удалось получить левую группу у SortMergeJoinOp")
                val leftExpr = leftGroup.bestExpression
                    ?: throw IllegalStateException("У левой группы для SortMergeJoinOp отсутствует bestExpression")
                println("    материализуем левую ветку (${leftExpr.op})")
                val (leftRaw, leftSchema) = materializeExpression(leftExpr)
                println("    → левый «сырой» файл: $leftRaw")

                // (б) Материализуем правую ветку «сырую»
                val rightGroup = expr.children.getOrNull(1)
                    ?: throw IllegalStateException("Не удалось получить правую группу у SortMergeJoinOp")
                val rightExpr = rightGroup.bestExpression
                    ?: throw IllegalStateException("У правой группы для SortMergeJoinOp отсутствует bestExpression")
                println("    материализуем правую ветку (${rightExpr.op})")
                val (rightRaw, rightSchema) = materializeExpression(rightExpr)
                println("    → правый «сырой» файл: $rightRaw")

                val leftSorted: Path
                val rightSorted: Path

                if (op.alreadySorted) {
                    println("    alreadySorted=true → используем исходные файлы без дополнительной сортировки")
                    leftSorted = leftRaw
                    rightSorted = rightRaw
                } else {
                    println("    alreadySorted=false → сортируем оба входа")
                    val leftTmp = Files.createTempFile(tempWorkDir, "smj_left_sorted_", ".tbl")
                    println("      сортируем LEFT: input=$leftRaw, key='${op.outerKey}', output=$leftTmp")
                    simpleSortByColumn(
                        input = leftRaw,
                        schema = leftSchema,
                        columnName = op.outerKey,
                        output = leftTmp
                    )
                    val rightTmp = Files.createTempFile(tempWorkDir, "smj_right_sorted_", ".tbl")
                    println("      сортируем RIGHT: input=$rightRaw, key='${op.innerKey}', output=$rightTmp")
                    simpleSortByColumn(
                        input = rightRaw,
                        schema = rightSchema,
                        columnName = op.innerKey,
                        output = rightTmp
                    )
                    println("      удаляем исходные сырые файлы: $leftRaw, $rightRaw")
                    Files.deleteIfExists(leftRaw)
                    Files.deleteIfExists(rightRaw)
                    leftSorted = leftTmp
                    rightSorted = rightTmp
                }

                val outFile = Files.createTempFile(tempWorkDir, "sortmergejoin_", ".tbl")
                println("    создаём временный файл-вывода: $outFile")
                println("    leftSorted = $leftSorted, rightSorted = $rightSorted")

                println("    запускаем MergeJoinIterator...")
                val iterator = MergeJoinIterator(
                    leftIter = ScanIterator(leftSorted, leftSchema),
                    rightIter = ScanIterator(rightSorted, rightSchema),
                    joinType = op.joinType,
                    leftKey = op.outerKey,
                    rightKey = op.innerKey,
                    condOp = op.condOp
                )
                writeIteratorToFile(iterator, outFile, leftSchema, rightSchema)

                if (!op.alreadySorted) {
                    println("    удаляем промежуточные отсортированные файлы: $leftSorted, $rightSorted")
                    Files.deleteIfExists(leftSorted)
                    Files.deleteIfExists(rightSorted)
                }

                println("    → SortMergeJoin результат записан в $outFile")

                val joinedColumns = leftSchema.columns + rightSchema.columns
                val joinedSchema = YSchema(table = "join", columns = joinedColumns)

                Pair(outFile, joinedSchema)
            }

            else -> {
                throw IllegalStateException("Неизвестный оператор в Executor: $op")
            }
        }
    }

    private fun simpleSortByColumn(
        input: Path,
        schema: YSchema,
        columnName: String,
        output: Path
    ) {
        val colIdx = schema.columns.indexOfFirst { it.name == columnName }
        require(colIdx >= 0) { "Колонка '$columnName' не найдена в схеме" }

        val lines = Files.readAllLines(input, StandardCharsets.UTF_8)

        val sorted = lines.filter { it.isNotEmpty() }
            .sortedWith(compareBy { line ->
                val tokens = line.split('|')
                tokens[colIdx]
            })

        Files.newBufferedWriter(
            output, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
        ).use { writer ->
            for (ln in sorted) {
                writer.write(ln)
                writer.newLine()
            }
        }
    }

    companion object {
        fun writeIteratorToFile(
            iterator: TupleIterator,
            output: Path,
            leftSchema: YSchema,
            rightSchema: YSchema
        ) {
            var rows = 0L
            Files.newBufferedWriter(
                output, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
            ).use { writer ->
                iterator.open()
                val columnsOrder = leftSchema.columns + rightSchema.columns
                while (true) {
                    val row: Row = iterator.next() ?: break
                    val line = columnsOrder.joinToString(separator = "|") { col ->
                        val v = row.columns[col.name]
                        v?.toString() ?: ""
                    }
                    writer.write(line)
                    writer.newLine()
                    rows++
                }
                iterator.close()
                val joinedSchema = YSchema(
                    table = output.fileName.toString(),
                    columns = leftSchema.columns + rightSchema.columns
                )
                registerStats(output, joinedSchema, rows)
            }
        }

        private fun registerStats(file: Path, schema: YSchema, rows: Long? = null) {
            val nrows = rows ?: Files.lines(file).count()
            val totalBytes = Files.size(file)
            val st = Stats(
                rows = nrows,
                filePages = ceil(totalBytes / 8192.0).toLong(),
                totalBytes = totalBytes,
                rowSizeBytes = if (nrows > 0) (totalBytes / nrows) else 0,
                col = mutableMapOf()
            ).apply { finalizeStats() }
            Catalog.putStats(file.fileName.toString(), st)
        }

        fun synthesizeJoinStats(
            left: Stats,
            right: Stats,
            joinRows: Long
        ): Stats {
            val st = Stats()
            st.rows = joinRows
            st.rowSizeBytes = (left.avgRowSize + right.avgRowSize).toLong()
            st.totalBytes = st.rows * st.rowSizeBytes
            st.filePages = ceil(st.totalBytes / 8192.0).toLong()

            /* ───────── копируем ColStats из обеих сторон ───────── */
            left.col.forEach { (k, v) -> st.col[k] = v.copy() }
            right.col.forEach { (k, v) -> st.col.putIfAbsent(k, v.copy()) }

            /* можно попробовать грубо актуализировать ndv для join-ключей: */
            //val key = …         // если знаете, какой столбец был джойн-ключом
            //st.col[key]?.ndv = min(left.col[key]?.ndv ?: 0, right.col[key]?.ndv ?: 0)

            st.finalizeStats()
            return st
        }
    }
}
