package org.bmstu.execution

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.test.assertEquals
import kotlin.test.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import logical.Expression
import logical.Group
import operators.*
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.ConditionOperator
import org.bmstu.reader.YSchema
import org.bmstu.reader.Col
import org.bmstu.reader.CType
import org.bmstu.tables.Row
import org.bmstu.joins.algorithms.ScanIterator

private fun writeTbl(path: Path, lines: List<String>) {
    Files.newBufferedWriter(
        path,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING
    ).use { w ->
        lines.forEach {
            w.write(it)
            w.newLine()
        }
    }
}

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


class ExecutorAdditionalTests {

    @Test
    fun `hash join inner returns only matching rows`(@TempDir tmp: Path) {
        // L: (1,A), (2,B), (3,C)
        val fileL = tmp.resolve("L.tbl")
        writeTbl(fileL, listOf("1|A", "2|B", "3|C"))

        // R: (2|X), (3|Y), (4|Z)
        val fileR = tmp.resolve("R.tbl")
        writeTbl(fileR, listOf("2|X", "3|Y", "4|Z"))

        val schemaL = YSchema("L", listOf(Col("idL", CType.INT), Col("vL", CType.STRING)))
        val schemaR = YSchema("R", listOf(Col("idR", CType.INT), Col("vR", CType.STRING)))

        val tablePaths = mapOf("L" to fileL, "R" to fileR)
        val schemas = mapOf("L" to schemaL, "R" to schemaR)

        // Scan L
        val scanL = Expression(op = ScanOp("L", card = 3), children = emptyList())
        val groupL = Group(setOf("L")).apply { bestExpression = scanL }

        // Scan R
        val scanR = Expression(op = ScanOp("R", card = 3), children = emptyList())
        val groupR = Group(setOf("R")).apply { bestExpression = scanR }

        // HashJoin INNER: L ⋈ R ON idL = idR, build = L, probe = R
        val joinOp = HashJoinOp(
            joinType = JoinCommand.INNER,
            outerKey = "idL",
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = hashJoinExpr }

        val executor = Executor(tablePaths, schemas)
        val outFile = tmp.resolve("out_hash_inner.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // Схема результата: idL|vL|idR|vR
        val schemaJoined = YSchema(
            table = "JOINED",
            columns = listOf(
                Col("idL", CType.INT),
                Col("vL", CType.STRING),
                Col("idR", CType.INT),
                Col("vR", CType.STRING)
            )
        )
        val rows = readAllRows(outFile, schemaJoined)

        // Должно быть ровно 2 строки (для id=2,3)
        assertEquals(2, rows.size)

        val sorted = rows.sortedBy { it.columns["idL"] as Int }
        assertEquals(2, sorted[0].columns["idL"] as Int)
        assertEquals("B", sorted[0].columns["vL"])
        assertEquals(2, sorted[0].columns["idR"] as Int)
        assertEquals("X", sorted[0].columns["vR"])

        assertEquals(3, sorted[1].columns["idL"] as Int)
        assertEquals("C", sorted[1].columns["vL"])
        assertEquals(3, sorted[1].columns["idR"] as Int)
        assertEquals("Y", sorted[1].columns["vR"])
    }

    @Test
    fun `hash join left outer returns all left rows with nulls where no match`(@TempDir tmp: Path) {
        // L: (1,A), (2,B), (3,C)
        val fileL = tmp.resolve("L.tbl")
        writeTbl(fileL, listOf("1|A", "2|B", "3|C"))

        // R: (2,X), (3,Y)
        val fileR = tmp.resolve("R.tbl")
        writeTbl(fileR, listOf("2|X", "3|Y"))

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

        // HashJoin LEFT: L ⋈ R ON idL = idR, build = L, probe = R
        val joinOp = HashJoinOp(
            joinType = JoinCommand.LEFT,
            outerKey = "idL",
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = hashJoinExpr }

        val executor = Executor(tablePaths, schemas)
        val outFile = tmp.resolve("out_hash_left.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // Схема результата: idL|vL|idR|vR
        val schemaJoined = YSchema(
            table = "JOINED",
            columns = listOf(
                Col("idL", CType.INT),
                Col("vL", CType.STRING),
                Col("idR", CType.INT),
                Col("vR", CType.STRING)
            )
        )
        val rows = readAllRows(outFile, schemaJoined)

        // Должно быть ровно 3 строки (idL=1,2,3)
        assertEquals(3, rows.size)

        val sorted = rows.sortedBy { it.columns["idL"] as Int }

        // idL=1 → нет R → NULL|NULL
        val r1 = sorted[0]
        assertEquals(1, r1.columns["idL"] as Int)
        assertEquals("A", r1.columns["vL"])
        assertNull(r1.columns["idR"])
        assertNull(r1.columns["vR"])

        // idL=2 → (2|B)⋈(2|X)
        val r2 = sorted[1]
        assertEquals(2, r2.columns["idL"] as Int)
        assertEquals("B", r2.columns["vL"])
        assertEquals(2, r2.columns["idR"] as Int)
        assertEquals("X", r2.columns["vR"])

        // idL=3 → (3|C)⋈(3|Y)
        val r3 = sorted[2]
        assertEquals(3, r3.columns["idL"] as Int)
        assertEquals("C", r3.columns["vL"])
        assertEquals(3, r3.columns["idR"] as Int)
        assertEquals("Y", r3.columns["vR"])
    }

    @Test
    fun `hash join right outer returns all right rows with nulls where no match`(@TempDir tmp: Path) {

        // L: (1,A), (3,C)
        val fileL = tmp.resolve("L.tbl")
        writeTbl(fileL, listOf("1|A", "3|C"))

        // R: (1,X), (2,Y), (3,Z)
        val fileR = tmp.resolve("R.tbl")
        writeTbl(fileR, listOf("1|X", "2|Y", "3|Z"))

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

        // HashJoin RIGHT: build = R, probe = L, buildOnLeft = false
        val joinOp = HashJoinOp(
            joinType = JoinCommand.RIGHT,
            outerKey = "idL",
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = hashJoinExpr }

        val executor = Executor(tablePaths, schemas)
        val outFile = tmp.resolve("out_hash_right.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // Схема результата: idL|vL|idR|vR
        val schemaJoined = YSchema(
            table = "JOINED",
            columns = listOf(
                Col("idL", CType.INT),
                Col("vL", CType.STRING),
                Col("idR", CType.INT),
                Col("vR", CType.STRING)
            )
        )
        val rows = readAllRows(outFile, schemaJoined)

        // Должно быть ровно 3 строки (idR=1,2,3)
        assertEquals(3, rows.size)

        val sorted = rows.sortedBy { it.columns["idR"] as Int }

        // idR=1 → (1,A)⋈(1,X)
        val r1 = sorted[0]
        assertEquals(1, r1.columns["idL"] as Int)
        assertEquals("A", r1.columns["vL"])
        assertEquals(1, r1.columns["idR"] as Int)
        assertEquals("X", r1.columns["vR"])

        // idR=2 → NULL⋈(2,Y)
        val r2 = sorted[1]
        assertNull(r2.columns["idL"])
        assertNull(r2.columns["vL"])
        assertEquals(2, r2.columns["idR"] as Int)
        assertEquals("Y", r2.columns["vR"])

        // idR=3 → (3,C)⋈(3,Z)
        val r3 = sorted[2]
        assertEquals(3, r3.columns["idL"] as Int)
        assertEquals("C", r3.columns["vL"])
        assertEquals(3, r3.columns["idR"] as Int)
        assertEquals("Z", r3.columns["vR"])
    }

    @Test
    fun `hash join full outer returns all rows with nulls on both sides`(@TempDir tmp: Path) {
        // L: (1,A), (3,C)
        val fileL = tmp.resolve("L.tbl")
        writeTbl(fileL, listOf("1|A", "3|C"))

        // R: (1,X), (2,Y), (4,Z)
        val fileR = tmp.resolve("R.tbl")
        writeTbl(fileR, listOf("1|X", "2|Y", "4|Z"))

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

        // HashJoin FULL: для FULL достаточно buildOnLeft=true или false, не влияет на логику фаз
        val joinOp = HashJoinOp(
            joinType = JoinCommand.FULL,
            outerKey = "idL",
            innerKey = "idR",
            buildSideLeft = true,
            partitioned = false
        )
        val hashJoinExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = hashJoinExpr }

        val executor = Executor(tablePaths, schemas)
        val outFile = tmp.resolve("out_hash_full.tbl")
        executor.executePhysicalPlanToFile(hashJoinExpr, outFile)

        // Схема результата: idL|vL|idR|vR
        val schemaJoined = YSchema(
            table = "JOINED",
            columns = listOf(
                Col("idL", CType.INT),
                Col("vL", CType.STRING),
                Col("idR", CType.INT),
                Col("vR", CType.STRING)
            )
        )
        val rows = readAllRows(outFile, schemaJoined)

        // Должно быть ровно 4 строки:
        //  id=1 match, id=2 (NULL, Y), id=3 (3,C,NULL), id=4 (NULL,Z)
        assertEquals(4, rows.size)

        // Проверим “множество” пар (idL,idR) вне порядка
        val pairs =
            rows.map { (it.columns["idL"]?.toString() ?: "N") + "/" + (it.columns["idR"]?.toString() ?: "N") }.toSet()
        // Ожидаем: {"1/1", "N/2", "3/N", "N/4"}
        assertEquals(
            setOf("1/1", "N/2", "3/N", "N/4"),
            pairs
        )
    }


    ////////////////////////////////////////////////////////////////////////////////
    // 2) Block-Nested-Loop Join: INNER, LEFT, RIGHT, FULL
    ////////////////////////////////////////////////////////////////////////////////

    @Test
    fun `block NL join inner returns only matching rows`(@TempDir tmp: Path) {
        // L: (1,A), (2,B), (3,C)
        val fileL = tmp.resolve("L.tbl")
        writeTbl(fileL, listOf("1|A", "2|B", "3|C"))

        // R: (2,X), (3,Y), (4,Z)
        val fileR = tmp.resolve("R.tbl")
        writeTbl(fileR, listOf("2|X", "3|Y", "4|Z"))

        val schemaL = YSchema("L", listOf(Col("idL", CType.INT), Col("vL", CType.STRING)))
        val schemaR = YSchema("R", listOf(Col("idR", CType.INT), Col("vR", CType.STRING)))

        val tablePaths = mapOf("L" to fileL, "R" to fileR)
        val schemas = mapOf("L" to schemaL, "R" to schemaR)

        // Scan L
        val scanL = Expression(op = ScanOp("L", card = 3), children = emptyList())
        val groupL = Group(setOf("L")).apply { bestExpression = scanL }

        // Scan R
        val scanR = Expression(op = ScanOp("R", card = 3), children = emptyList())
        val groupR = Group(setOf("R")).apply { bestExpression = scanR }

        // BlockNLJoin INNER: build=L, probe=R
        val joinOp = BlockNLJoinOp(
            joinType  = JoinCommand.INNER,
            outerKey  = "idL",
            innerKey  = "idR",
            outerLeft = true,
            condOp    = ConditionOperator.EQUALS
        )
        val genericInner = Expression(
            op = JoinOp(JoinCommand.INNER, "idL", "idR", ConditionOperator.EQUALS),
            children = listOf(groupL, groupR)
        )
        // Прямо из genericInner сделаем BlockNLJoinOp
        val blockExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = blockExpr }

        val executor = Executor(tablePaths, schemas)
        val outFile = tmp.resolve("out_blocknl_inner.tbl")
        executor.executePhysicalPlanToFile(blockExpr, outFile)

        val schemaJoined = YSchema(
            "JOINED", listOf(
                Col("idL", CType.INT), Col("vL", CType.STRING),
                Col("idR", CType.INT), Col("vR", CType.STRING)
            )
        )
        val rows = readAllRows(outFile, schemaJoined)
        assertEquals(2, rows.size)

        val sorted = rows.sortedBy { it.columns["idL"] as Int }
        assertEquals(2, sorted[0].columns["idL"] as Int)
        assertEquals("B", sorted[0].columns["vL"])
        assertEquals(2, sorted[0].columns["idR"] as Int)
        assertEquals("X", sorted[0].columns["vR"])

        assertEquals(3, sorted[1].columns["idL"] as Int)
        assertEquals("C", sorted[1].columns["vL"])
        assertEquals(3, sorted[1].columns["idR"] as Int)
        assertEquals("Y", sorted[1].columns["vR"])
    }

    @Test
    fun `block NL join left outer returns all left rows with nulls`(@TempDir tmp: Path) {
        // L: (1,A), (2,B), (3,C)
        val fileL = tmp.resolve("L.tbl")
        writeTbl(fileL, listOf("1|A", "2|B", "3|C"))

        // R: (2,X), (3,Y)
        val fileR = tmp.resolve("R.tbl")
        writeTbl(fileR, listOf("2|X", "3|Y"))

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

        // BlockNLJoin LEFT: build=L, probe=R
        val joinOp = BlockNLJoinOp(
            JoinCommand.LEFT,
            "idL",
            "idR",
            outerLeft = true,
            ConditionOperator.EQUALS
        )
        val blockExpr = Expression(op = joinOp, children = listOf(groupL, groupR))
        Group(setOf("L", "R")).apply { bestExpression = blockExpr }

        val executor = Executor(tablePaths, schemas)
        val outFile = tmp.resolve("out_blocknl_left.tbl")
        executor.executePhysicalPlanToFile(blockExpr, outFile)

        val schemaJoined = YSchema(
            "JOINED", listOf(
                Col("idL", CType.INT), Col("vL", CType.STRING),
                Col("idR", CType.INT), Col("vR", CType.STRING)
            )
        )
        val rows = readAllRows(outFile, schemaJoined)
        assertEquals(3, rows.size)

        val sorted = rows.sortedBy { it.columns["idL"] as Int }

        // idL=1 → NULL
        assertEquals(1, sorted[0].columns["idL"])
        assertEquals("A", sorted[0].columns["vL"])
        assertNull(sorted[0].columns["idR"])
        assertNull(sorted[0].columns["vR"])

        // idL=2 match
        assertEquals(2, sorted[1].columns["idL"])
        assertEquals("B", sorted[1].columns["vL"])
        assertEquals(2, sorted[1].columns["idR"])
        assertEquals("X", sorted[1].columns["vR"])

        // idL=3 match
        assertEquals(3, sorted[2].columns["idL"])
        assertEquals("C", sorted[2].columns["vL"])
        assertEquals(3, sorted[2].columns["idR"])
        assertEquals("Y", sorted[2].columns["vR"])
    }
}
