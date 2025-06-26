package org.bmstu.execution

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import operators.HashJoinOp
import operators.ScanOp
import org.bmstu.joins.JoinCommand
import org.bmstu.reader.CType
import org.bmstu.reader.Col
import org.bmstu.reader.YSchema
import org.bmstu.tables.Row
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.algorithms.ScanIterator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import logical.Expression
import logical.Group
import operators.SortMergeJoinOp

/**
 * Вспомогательная функция: считывает все строки из .tbl-файла через ScanIterator
 * и возвращает список Row.
 */
private fun readAllRows(path: Path, schema: YSchema): List<Row> {
    val rows = mutableListOf<Row>()
    ScanIterator(path, schema).run {
        open()
        while (true) {
            val r = next() ?: break
            rows += r
        }
        close()
    }
    return rows
}

class ExecutorTest {

    @Test
    fun `scan executor returns all rows to file`(@TempDir tempDir: Path) {
        // 1) Создаём временный файл T.tbl
        val fileT = tempDir.resolve("T.tbl")
        Files.newBufferedWriter(
            fileT,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use { writer ->
            writer.write("1|Alice")
            writer.newLine()
            writer.write("2|Bob")
            writer.newLine()
        }

        // 2) Определяем схему для таблицы T
        val schemaT = YSchema(
            table = "T",
            columns = listOf(
                Col(name = "id", type = CType.INT),
                Col(name = "name", type = CType.STRING)
            )
        )

        // 3) Мапы для Executor
        val tablePaths = mapOf("T" to fileT)
        val schemas = mapOf("T" to schemaT)

        // 4) Физическое Expression — просто ScanOp без детей
        val scanExpr = Expression(
            op = ScanOp("T", card = 2), // card не используется Executor'ом при Scan
            children = emptyList()
        )
        // Группа для скана таблицы T
        val groupT = Group(setOf("T")).apply {
            bestExpression = scanExpr
        }

        // 5) Запускаем Executor, записываем в outFile
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_scan.tbl")
        executor.executePhysicalPlanToFile(scanExpr, outFile)

        // 6) Считываем результат из файла и проверяем
        val result = readAllRows(outFile, schemaT)
        assertEquals(2, result.size)

        // Проверяем первую строку
        val r1 = result[0]
        assertEquals(2, r1.columns.size)
        assertEquals(1, (r1.columns["id"] as Int))
        assertEquals("Alice", (r1.columns["name"] as String))

        // Проверяем вторую строку
        val r2 = result[1]
        assertEquals(2, r2.columns.size)
        assertEquals(2, (r2.columns["id"] as Int))
        assertEquals("Bob", (r2.columns["name"] as String))
    }

    /**
     * Тестирует, что Executor корректно выполняет HashJoin между
     * двумя маленькими таблицами и записывает результат в файл.
     */
    @Test
    fun `hash join executor returns joined rows to file`(@TempDir tempDir: Path) {
        // 1) Создаём файлы L.tbl и R.tbl
        val fileL = tempDir.resolve("L.tbl")
        Files.newBufferedWriter(
            fileL,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use { writer ->
            writer.write("1|X1"); writer.newLine()
            writer.write("2|X2"); writer.newLine()
            writer.write("3|X3"); writer.newLine()
        }

        val fileR = tempDir.resolve("R.tbl")
        Files.newBufferedWriter(
            fileR,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use { writer ->
            writer.write("2|Y2"); writer.newLine()
            writer.write("3|Y3"); writer.newLine()
            writer.write("4|Y4"); writer.newLine()
        }

        // 2) Определяем схемы для L и R
        val schemaL = YSchema(
            table = "L",
            columns = listOf(
                Col(name = "idL", type = CType.INT),
                Col(name = "valueL", type = CType.STRING)
            )
        )
        val schemaR = YSchema(
            table = "R",
            columns = listOf(
                Col(name = "idR", type = CType.INT),
                Col(name = "valueR", type = CType.STRING)
            )
        )

        // 3) Мапы для Executor
        val tablePaths = mapOf("L" to fileL, "R" to fileR)
        val schemas = mapOf("L" to schemaL, "R" to schemaR)

        // 4) Строим физическое Expression: HashJoinOp с двумя ScanOp

        // 4.1) Scan по L
        val scanL = Expression(
            op = ScanOp("L", card = 3),
            children = emptyList()
        )
        val groupL = Group(setOf("L")).apply {
            bestExpression = scanL
        }

        // 4.2) Scan по R
        val scanR = Expression(
            op = ScanOp("R", card = 3),
            children = emptyList()
        )
        val groupR = Group(setOf("R")).apply {
            bestExpression = scanR
        }

        // 4.3) HashJoin L ⋈ R
        val joinOp = HashJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "idL",
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(
            op = joinOp,
            children = listOf(groupL, groupR)
        )
        val groupLR = Group(setOf("L", "R")).apply {
            bestExpression = hashJoinExpr
        }

        // 5) Запускаем Executor
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_hashjoin.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // 6) Считываем и проверяем результат
        // Схема результирующего join объединяет idL|valueL|idR|valueR
        val schemaJoined = YSchema(
            table = "joined",
            columns = listOf(
                Col(name = "idL", type = CType.INT),
                Col(name = "valueL", type = CType.STRING),
                Col(name = "idR", type = CType.INT),
                Col(name = "valueR", type = CType.STRING)
            )
        )
        val joinedRows = readAllRows(outFile, schemaJoined)
        assertEquals(2, joinedRows.size)

        // Отсортируем по "idL" для детерминированного порядка
        val sorted = joinedRows.sortedBy { (it.columns["idL"] as Int) }
        val first = sorted[0]
        val second = sorted[1]

        // Проверяем первую пару: idL=2, X2, idR=2, Y2
        assertEquals(2, (first.columns["idL"] as Int))
        assertEquals("X2", (first.columns["valueL"] as String))
        assertEquals(2, (first.columns["idR"] as Int))
        assertEquals("Y2", (first.columns["valueR"] as String))

        // Проверяем вторую пару: idL=3, X3, idR=3, Y3
        assertEquals(3, (second.columns["idL"] as Int))
        assertEquals("X3", (second.columns["valueL"] as String))
        assertEquals(3, (second.columns["idR"] as Int))
        assertEquals("Y3", (second.columns["valueR"] as String))
    }

    /**
     * Тестирует выполнение цепочки из двух HashJoin-ов (A⋈B ⋈ C),
     * записывая результат в файл и затем проверяя содержимое.
     */
    @Test
    fun `multiple hash joins executor returns correct rows to file`(@TempDir tempDir: Path) {
        // 1) Создаём файлы A.tbl, B.tbl, C.tbl
        val fileA = tempDir.resolve("A.tbl")
        Files.newBufferedWriter(
            fileA,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use { writer ->
            writer.write("1|X"); writer.newLine()
            writer.write("2|Y"); writer.newLine()
        }

        val fileB = tempDir.resolve("B.tbl")
        Files.newBufferedWriter(
            fileB,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use { writer ->
            writer.write("1|U"); writer.newLine()
            writer.write("2|V"); writer.newLine()
            writer.write("3|W"); writer.newLine()
        }

        val fileC = tempDir.resolve("C.tbl")
        Files.newBufferedWriter(
            fileC,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use { writer ->
            writer.write("2|Z"); writer.newLine()
            writer.write("3|W"); writer.newLine()
        }

        // 2) Определяем схемы YSchema для A, B, C
        val schemaA = YSchema(
            table = "A",
            columns = listOf(
                Col(name = "idA", type = CType.INT),
                Col(name = "valA", type = CType.STRING)
            )
        )
        val schemaB = YSchema(
            table = "B",
            columns = listOf(
                Col(name = "idB", type = CType.INT),
                Col(name = "valB", type = CType.STRING)
            )
        )
        val schemaC = YSchema(
            table = "C",
            columns = listOf(
                Col(name = "idC", type = CType.INT),
                Col(name = "valC", type = CType.STRING)
            )
        )

        // 3) Мапы для Executor
        val tablePaths = mapOf("A" to fileA, "B" to fileB, "C" to fileC)
        val schemas = mapOf("A" to schemaA, "B" to schemaB, "C" to schemaC)

        // 4) Строим физические Expression

        // 4.1) Scan по A
        val scanA = Expression(
            op = ScanOp("A", card = 2),
            children = emptyList()
        )
        val groupA = Group(setOf("A")).apply {
            bestExpression = scanA
        }

        // 4.2) Scan по B
        val scanB = Expression(
            op = ScanOp("B", card = 3),
            children = emptyList()
        )
        val groupB = Group(setOf("B")).apply {
            bestExpression = scanB
        }

        // 4.3) Первый HashJoin: A ⋈ B по A.idA = B.idB
        val joinABOp = HashJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "idA",
            innerKey = "idB",
            buildSideLeft = true,
            partitioned = false
        )
        val joinABExpr = Expression(
            op = joinABOp,
            children = listOf(groupA, groupB)
        )
        val groupAB = Group(setOf("A", "B")).apply {
            bestExpression = joinABExpr
        }

        // 4.4) Scan по C
        val scanC = Expression(
            op = ScanOp("C", card = 2),
            children = emptyList()
        )
        val groupC = Group(setOf("C")).apply {
            bestExpression = scanC
        }

        // 4.5) Второй HashJoin: (A⋈B) ⋈ C по idA = idC
        val joinFinalOp = HashJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "idA",
            innerKey = "idC",
            buildSideLeft = true,
            partitioned = false
        )
        val joinFinalExpr = Expression(
            op = joinFinalOp,
            children = listOf(groupAB, groupC)
        )
        val groupABC = Group(setOf("A", "B", "C")).apply {
            bestExpression = joinFinalExpr
        }

        // 5) Запускаем Executor
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_multiple.tbl")
        executor.executePhysicalPlanToFile(joinFinalExpr, outFile)

        // 6) Считываем и проверяем результат
        // Схема результирующего join: idA|valA|idB|valB|idC|valC
        val schemaJoined = YSchema(
            table = "ABC",
            columns = listOf(
                Col(name = "idA", type = CType.INT),
                Col(name = "valA", type = CType.STRING),
                Col(name = "idB", type = CType.INT),
                Col(name = "valB", type = CType.STRING),
                Col(name = "idC", type = CType.INT),
                Col(name = "valC", type = CType.STRING)
            )
        )
        val joinedRows = readAllRows(outFile, schemaJoined)

        assertEquals(1, joinedRows.size)

        val actualTokens = joinedRows[0].columns.values.map { it.toString() }
        assertEquals(listOf("2", "Y", "2", "V", "2", "Z"), actualTokens)
    }

    @Test
    fun `hash join left outer returns all left rows with nulls`(@TempDir tempDir: Path) {
        // Таблица L: 1|A, 2|B, 3|C
        val fileL = tempDir.resolve("L.tbl")
        Files.newBufferedWriter(
            fileL,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("1|A"); it.newLine()
            it.write("2|B"); it.newLine()
            it.write("3|C"); it.newLine()
        }
        // Таблица R: 2|X, 3|Y
        val fileR = tempDir.resolve("R.tbl")
        Files.newBufferedWriter(
            fileR,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("2|X"); it.newLine()
            it.write("3|Y"); it.newLine()
        }

        val schemaL = YSchema("L", listOf(Col("idL", CType.INT), Col("vL", CType.STRING)))
        val schemaR = YSchema("R", listOf(Col("idR", CType.INT), Col("vR", CType.STRING)))
        val tablePaths = mapOf("L" to fileL, "R" to fileR)
        val schemas = mapOf("L" to schemaL, "R" to schemaR)

        // Scan L
        val scanL = Expression(op = ScanOp("L", card = 3), children = emptyList())
        val groupL = Group(setOf("L")).apply { bestExpression = scanL }
        // Scan R
        val scanR = Expression(op = ScanOp("R", card = 2), children = emptyList())
        val groupR = Group(setOf("R")).apply { bestExpression = scanR }
        // HashJoin LEFT
        val joinOp = HashJoinOp(
            joinType = JoinCommand.LEFT,
            outerKey = "idL",
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = hashJoinExpr }

        // Запускаем
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_left.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // Схема результата: idL|vL|idR|vR
        val schemaJoined = YSchema("LR", listOf(
            Col("idL", CType.INT), Col("vL", CType.STRING),
            Col("idR", CType.INT), Col("vR", CType.STRING)
        ))
        val rows = readAllRows(outFile, schemaJoined)
        // Должно быть ровно 3 строки (для idL=1,2,3)
        assertEquals(3, rows.size)

        // Проверим содержимое (отсортируем по idL)
        val sorted = rows.sortedBy { (it.columns["idL"] as Int) }

        // idL=1 → нет совпадения в R, ожидаем null'ы
        val r1 = sorted[0]
        assertEquals(1, (r1.columns["idL"] as Int))
        assertEquals("A", (r1.columns["vL"] as String))
        assertEquals(null, r1.columns["idR"])
        assertEquals(null, r1.columns["vR"])

        // idL=2 → есть совпадение (2|X)
        val r2 = sorted[1]
        assertEquals(2, (r2.columns["idL"] as Int))
        assertEquals("B", (r2.columns["vL"] as String))
        assertEquals(2, (r2.columns["idR"] as Int))
        assertEquals("X", (r2.columns["vR"] as String))

        // idL=3 → есть совпадение (3|Y)
        val r3 = sorted[2]
        assertEquals(3, (r3.columns["idL"] as Int))
        assertEquals("C", (r3.columns["vL"] as String))
        assertEquals(3, (r3.columns["idR"] as Int))
        assertEquals("Y", (r3.columns["vR"] as String))
    }

    /**
     * 2) HashJoin RIGHT — проверяем, что для отсутствующих в L строк из R появятся null’ы
     */
    @Test
    fun `hash join right outer returns all right rows with nulls`(@TempDir tempDir: Path) {
        // Таблица L: 1|A, 3|C
        val fileL = tempDir.resolve("L.tbl")
        Files.newBufferedWriter(
            fileL,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("1|A"); it.newLine()
            it.write("3|C"); it.newLine()
        }
        // Таблица R: 1|X, 2|Y, 3|Z
        val fileR = tempDir.resolve("R.tbl")
        Files.newBufferedWriter(
            fileR,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("1|X"); it.newLine()
            it.write("2|Y"); it.newLine()
            it.write("3|Z"); it.newLine()
        }

        val schemaL = YSchema("L", listOf(Col("idL", CType.INT), Col("vL", CType.STRING)))
        val schemaR = YSchema("R", listOf(Col("idR", CType.INT), Col("vR", CType.STRING)))
        val tablePaths = mapOf("L" to fileL, "R" to fileR)
        val schemas = mapOf("L" to schemaL, "R" to schemaR)

        // Scan L
        val scanL = Expression(op = ScanOp("L", card = 2), children = emptyList())
        val groupL = Group(setOf("L")).apply { bestExpression = scanL }
        // Scan R
        val scanR = Expression(op = ScanOp("R", card = 3), children = emptyList())
        val groupR = Group(setOf("R")).apply { bestExpression = scanR }
        // HashJoin RIGHT
        val joinOp = HashJoinOp(
            joinType = JoinCommand.RIGHT,
            outerKey = "idL",  // outer - правая таблица в этом операторе
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = hashJoinExpr }

        // Запускаем
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_right.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // Схема результата: idL|vL|idR|vR
        val schemaJoined = YSchema("LR", listOf(
            Col("idL", CType.INT), Col("vL", CType.STRING),
            Col("idR", CType.INT), Col("vR", CType.STRING)
        ))
        val rows = readAllRows(outFile, schemaJoined)
        // Должно быть ровно 3 строки (для idR=1,2,3)
        assertEquals(3, rows.size)

        // Проверим содержимое (отсортируем по idR)
        val sorted = rows.sortedBy { (it.columns["idR"] as Int?) ?: -1 }

        // idR=1 → совпадение (1|A)
        val r1 = sorted[0]
        assertEquals(1, (r1.columns["idL"] as Int))
        assertEquals("A", (r1.columns["vL"] as String))
        assertEquals(1, (r1.columns["idR"] as Int))
        assertEquals("X", (r1.columns["vR"] as String))

        // idR=2 → нет совпадения, ожидаем null для L
        val r2 = sorted[1]
        assertEquals(null, r2.columns["idL"])
        assertEquals(null, r2.columns["vL"])
        assertEquals(2, (r2.columns["idR"] as Int))
        assertEquals("Y", (r2.columns["vR"] as String))

        // idR=3 → совпадение (3|C)
        val r3 = sorted[2]
        assertEquals(3, (r3.columns["idL"] as Int))
        assertEquals("C", (r3.columns["vL"] as String))
        assertEquals(3, (r3.columns["idR"] as Int))
        assertEquals("Z", (r3.columns["vR"] as String))
    }

    /**
     * 3) HashJoin FULL — проверяем, что извлекаются все строки из обеих таблиц,
     *    а отсутствующие заполняются null’ами
     */
    @Test
    fun `hash join full outer returns all rows with nulls`(@TempDir tempDir: Path) {
        // Таблица L: 1|A, 3|C
        val fileL = tempDir.resolve("L.tbl")
        Files.newBufferedWriter(
            fileL,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("1|A"); it.newLine()
            it.write("3|C"); it.newLine()
        }
        // Таблица R: 1|X, 2|Y
        val fileR = tempDir.resolve("R.tbl")
        Files.newBufferedWriter(
            fileR,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("1|X"); it.newLine()
            it.write("2|Y"); it.newLine()
        }

        val schemaL = YSchema("L", listOf(Col("idL", CType.INT), Col("vL", CType.STRING)))
        val schemaR = YSchema("R", listOf(Col("idR", CType.INT), Col("vR", CType.STRING)))
        val tablePaths = mapOf("L" to fileL, "R" to fileR)
        val schemas = mapOf("L" to schemaL, "R" to schemaR)

        // Scan L
        val scanL = Expression(op = ScanOp("L", card = 2), children = emptyList())
        val groupL = Group(setOf("L")).apply { bestExpression = scanL }
        // Scan R
        val scanR = Expression(op = ScanOp("R", card = 2), children = emptyList())
        val groupR = Group(setOf("R")).apply { bestExpression = scanR }
        // HashJoin FULL
        val joinOp = HashJoinOp(
            joinType = JoinCommand.FULL,
            outerKey = "idL",
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = hashJoinExpr }

        // Запускаем
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_full.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // Схема результата: idL|vL|idR|vR
        val schemaJoined = YSchema("LR", listOf(
            Col("idL", CType.INT), Col("vL", CType.STRING),
            Col("idR", CType.INT), Col("vR", CType.STRING)
        ))
        val rows = readAllRows(outFile, schemaJoined)
        // Должно быть ровно 3 строки: id=1, id=2, id=3
        assertEquals(3, rows.size)

        // Проверим содержимое (отсортируем по idL or idR)
        val sorted = rows.sortedWith(compareBy<Row> {
            // берем idL, если null — idR
            (it.columns["idL"] as Int?) ?: (it.columns["idR"] as Int)
        })

        // id=1: совпадение
        val r1 = sorted[0]
        assertEquals(1, (r1.columns["idL"] as Int))
        assertEquals("A", (r1.columns["vL"] as String))
        assertEquals(1, (r1.columns["idR"] as Int))
        assertEquals("X", (r1.columns["vR"] as String))

        // id=2: только в R → null в L
        val r2 = sorted[1]
        assertEquals(null, r2.columns["idL"])
        assertEquals(null, r2.columns["vL"])
        assertEquals(2, (r2.columns["idR"] as Int))
        assertEquals("Y", (r2.columns["vR"] as String))

        // id=3: только в L → null в R
        val r3 = sorted[2]
        assertEquals(3, (r3.columns["idL"] as Int))
        assertEquals("C", (r3.columns["vL"] as String))
        assertEquals(null, r3.columns["idR"])
        assertEquals(null, r3.columns["vR"])
    }

    /**
     * 4) BlockNLJoin INNER — проверяем, что BlockNLJoin работает так же, как HashJoin
     */
    @Test
    fun `block-nl join inner returns joined rows to file`(@TempDir tempDir: Path) {
        // Таблица A: 1|X, 2|Y
        val fileA = tempDir.resolve("A.tbl")
        Files.newBufferedWriter(
            fileA,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("1|X"); it.newLine()
            it.write("2|Y"); it.newLine()
        }
        // Таблица B: 2|U, 3|V
        val fileB = tempDir.resolve("B.tbl")
        Files.newBufferedWriter(
            fileB,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("2|U"); it.newLine()
            it.write("3|V"); it.newLine()
        }

        val schemaA = YSchema("A", listOf(Col("idA", CType.INT), Col("vA", CType.STRING)))
        val schemaB = YSchema("B", listOf(Col("idB", CType.INT), Col("vB", CType.STRING)))
        val tablePaths = mapOf("A" to fileA, "B" to fileB)
        val schemas = mapOf("A" to schemaA, "B" to schemaB)

        // Scan A
        val scanA = Expression(op = ScanOp("A", card = 2), children = emptyList())
        val groupA = Group(setOf("A")).apply { bestExpression = scanA }
        // Scan B
        val scanB = Expression(op = ScanOp("B", card = 2), children = emptyList())
        val groupB = Group(setOf("B")).apply { bestExpression = scanB }
        // составим Expression: ищем совпадение idA=idB
        // нам придётся явно указать условие через вложенный Expression,
        // но в текущем Executor для BlockNLJoin мы не храним ключи и condOp, поэтому
        // просто проверим, что соединяются строки с id совпадающими.
        // Код Executor использует BlockNLJoinIterator(buildPath, probePath, joinType, buildKey, probeKey, condOp).
        // Поэтому нужно положить нужные поля в JoinOp.
        val blockOp = operators.BlockNLJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "idA",
            innerKey = "idB",
            outerLeft = true,
            condOp = ConditionOperator.EQUALS
        )
        val blockExpr = Expression(op = blockOp, children = listOf(groupA, groupB))
        Group(setOf("A", "B")).apply { bestExpression = blockExpr }

        // Запускаем
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_blocknl.tbl")
        executor.executePhysicalPlanToFile(blockExpr, outFile)

        // Схема: idA|vA|idB|vB
        val schemaJoined = YSchema("AB", listOf(
            Col("idA", CType.INT), Col("vA", CType.STRING),
            Col("idB", CType.INT), Col("vB", CType.STRING)
        ))
        val rows = readAllRows(outFile, schemaJoined)
        // Должна быть ровно одна строка: 2|Y|2|U
        assertEquals(1, rows.size)
        val r = rows[0]
        assertEquals(2, (r.columns["idA"] as Int))
        assertEquals("Y", (r.columns["vA"] as String))
        assertEquals(2, (r.columns["idB"] as Int))
        assertEquals("U", (r.columns["vB"] as String))
    }

    /**
     * 5) SortMergeJoin INNER — проверяем, что SortMergeJoin работает так же, как HashJoin
     */
    @Test
    fun `sort-merge join inner returns joined rows to file`(@TempDir tempDir: Path) {
        // Таблица A: 1|X, 2|Y, 3|Z
        val fileA = tempDir.resolve("A.tbl")
        Files.newBufferedWriter(
            fileA,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("1|X"); it.newLine()
            it.write("2|Y"); it.newLine()
            it.write("3|Z"); it.newLine()
        }
        // Таблица B: 2|U, 3|V, 4|W
        val fileB = tempDir.resolve("B.tbl")
        Files.newBufferedWriter(
            fileB,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        ).use {
            it.write("2|U"); it.newLine()
            it.write("3|V"); it.newLine()
            it.write("4|W"); it.newLine()
        }

        val schemaA = YSchema("A", listOf(Col("idA", CType.INT), Col("vA", CType.STRING)))
        val schemaB = YSchema("B", listOf(Col("idB", CType.INT), Col("vB", CType.STRING)))
        val tablePaths = mapOf("A" to fileA, "B" to fileB)
        val schemas = mapOf("A" to schemaA, "B" to schemaB)

        // Scan A
        val scanA = Expression(op = ScanOp("A", card = 3), children = emptyList())
        val groupA = Group(setOf("A")).apply { bestExpression = scanA }
        // Scan B
        val scanB = Expression(op = ScanOp("B", card = 3), children = emptyList())
        val groupB = Group(setOf("B")).apply { bestExpression = scanB }
        // SortMergeJoin INNER (alreadySorted=false → Executor сам отсортирует файлы)
        val joinOp = SortMergeJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "idA",
            innerKey = "idB",
            alreadySorted = false,
            condOp = ConditionOperator.EQUALS
        )
        val smjExpr = Expression(op = joinOp, children = listOf(groupA, groupB))
        Group(setOf("A", "B")).apply { bestExpression = smjExpr }

        // Запускаем
        val executor = Executor(tablePaths, schemas)
        val outFile = tempDir.resolve("out_sortmerge.tbl")
        executor.executePhysicalPlanToFile(smjExpr, outFile)

        // Схема: idA|vA|idB|vB
        val schemaJoined = YSchema("AB", listOf(
            Col("idA", CType.INT), Col("vA", CType.STRING),
            Col("idB", CType.INT), Col("vB", CType.STRING)
        ))
        val rows = readAllRows(outFile, schemaJoined)
        // Должно быть 2 строки: 2|Y|2|U и 3|Z|3|V
        assertEquals(2, rows.size)
        // Отсортируем по idA
        val sorted = rows.sortedBy { (it.columns["idA"] as Int) }
        val r1 = sorted[0]; val r2 = sorted[1]
        assertEquals(2, (r1.columns["idA"] as Int)); assertEquals("Y", (r1.columns["vA"] as String))
        assertEquals(2, (r1.columns["idB"] as Int)); assertEquals("U", (r1.columns["vB"] as String))
        assertEquals(3, (r2.columns["idA"] as Int)); assertEquals("Z", (r2.columns["vA"] as String))
        assertEquals(3, (r2.columns["idB"] as Int)); assertEquals("V", (r2.columns["vB"] as String))
    }
}
