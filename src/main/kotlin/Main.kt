package org.bmstu


import RuleHashJoin
import cost.CostModel
import java.io.File
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.time.measureTime
import logical.Engine
import logical.ExploreGroupJob
import logical.Expression
import logical.LogicalBuilder
import logical.Memo
import logical.OptimizeGroupJob
import org.bmstu.execution.Executor
import org.bmstu.indexes.BPlusTreeDiskBuilderInt
import org.bmstu.indexes.char.BPlusTreeDiskBuilderStr
import org.bmstu.joins.algorithms.MergeJoinIterator
import org.bmstu.parser.Parser
import org.bmstu.parser.Validator
import org.bmstu.reader.CType
import org.bmstu.reader.SchemasLoader.Companion.listResourceDir
import org.bmstu.reader.SchemasLoader.Companion.loadSchema
import org.bmstu.reader.TablesLoader.Companion.listTableNamesFromDir
import org.bmstu.reader.YSchema
import org.bmstu.runtime.TopPlansExecutor
import org.bmstu.tables.Table
import org.bmstu.util.TimingStats
import org.bmstu.util.UniqueSetByField
import rules.Rule
import rules.implementation.RuleBlockNL
import rules.implementation.RuleIndexNL
import rules.implementation.RuleSortMerge
import rules.transformation.RuleAssoc
import rules.transformation.RuleComm
import rules.transformation.RuleLeftToRight
import rules.transformation.RuleRightToLeft
import statistic.Catalog
import statistic.Stats
import statistic.gatherStats
import statistic.loadIndexMeta
import util.dumpAllStats

fun main() {
    val logFile = File("app.log")
    val fileOut = PrintStream(logFile.outputStream().buffered(), true)
    //System.setOut(fileOut)
    val schemasName = listResourceDir("schemas")
    println("Найдено YAML файлов: ${schemasName.size}")
    schemasName.forEach { println(" - $it") }
    val schemas = mutableSetOf<YSchema>()

    schemasName.forEach { file ->
        val schema = loadSchema("schemas/$file")
        println("Схема: ${schema.table} с $schema")
    }

    val tables = UniqueSetByField<Table>()
    val tablesPaths = listTableNamesFromDir("tables")
    println("Найдено таблиц: ${tablesPaths.size}")

    val tablePaths: MutableMap<String, Path> = mutableMapOf()
    val schemasMap: MutableMap<String, YSchema> = mutableMapOf()

    val allStats = mutableMapOf<String, Stats>()

    tablesPaths.forEach { tablePath ->
        val tableName = tablePath.fileName.toString().removeSuffix(".tbl")
        val schemaName = tablePath.fileName.toString().replace("tbl", "yml")
        val schemaPath = "schemas/$schemaName"
        if (schemaName in schemasName)
            tables.add(Table(tablePath.fileName.toString().substringBefore(".tbl")))

        if (schemaName in schemasName) {
            val schema = loadSchema(schemaPath)
            tablePaths[tableName] = tablePath
            schemasMap[tableName] = schema

            schemas.add(schema)
            println("Читаем таблицу ${tablePath.fileName} по схеме $schemaName")

            println("Собираем статистику для $tablePath …")
            var stats: Stats
            val duration = measureTime { stats = gatherStats(tablePath, schema) }
            println(duration)

            allStats[tableName] = stats
            Catalog.putStats(tableName, stats)

            schema.columns.filter { it.index }.forEach { col ->
                println("Создаём индекс для таблицы $tablePath для поля ${col.name}")
                val indexPath = Paths.get("build", "indexes", "${schema.table}_${col.name}.bpt")
                when (col.type) {
                    CType.INT -> {
                        BPlusTreeDiskBuilderInt.build(
                            tbl = tablePath,
                            schema = schema,
                            keyCol = col.name,
                            indexPath = indexPath
                        )
                        loadIndexMeta(indexPath, tablePath.fileName.toString().removeSuffix(".tbl"), col.name)
                    }

                    CType.CHAR -> {
                        BPlusTreeDiskBuilderStr.build(
                            tbl = tablePath,
                            schema = schema,
                            keyCol = col.name,
                            indexPath = indexPath,
                            keyLen = col.length ?: 64
                        )
                        loadIndexMeta(indexPath, tablePath.fileName.toString().removeSuffix(".tbl"), col.name)
                    }

                    else -> {
                        println("Тип ${col.type} для индексации не поддерживается.")
                    }
                }
            }


        } else {
            println("Схема для таблицы ${tablePath.fileName} не найдена.")
        }
    }
    dumpAllStats(allStats, format = "yaml")

    val validator = Validator(tables.getAll(), schemas)

    while (true) {
        print("> ")
        val input = readLine()?.trim() ?: continue
        when (input.lowercase()) {
            "exit" -> break
            "help" -> {
                printHelp();
                continue
            }
        }
        val joins = Parser.parseJoins(input)
        if (joins.isEmpty()) {
            println("Нет JOIN-ов"); continue
        }

        if (!validator.validate(joins))
            continue

        val memo = Memo()
        val rootG = LogicalBuilder.build(joins, memo)
        println(rootG)
        println(memo)

        val disabledPhysicalRules = listOf<Rule>()

        val engine = Engine(
            memo,
            logicalRules = listOf(RuleComm, RuleAssoc, RuleLeftToRight, RuleRightToLeft),
            physicalRules = listOf(RuleBlockNL, RuleHashJoin, RuleIndexNL, RuleSortMerge)
                .filter { !disabledPhysicalRules.contains(it) }
        )

        engine.jobs += OptimizeGroupJob(rootG)
        engine.run()
        val ts = LocalDateTime.now()
            .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
        val outDir = Paths.get("build", "plans").apply { Files.createDirectories(this) }

        val allPath = outDir.resolve("plans-all-$ts.txt")
        val sb = StringBuilder()
        rootG.expressions.forEachIndexed { idx, expr ->
            sb.append("=== Plan #${idx + 1} ===\n")
                .append(expr.makeLogicalPlanPretty())
                .append("\n")
        }
        Files.writeString(allPath, sb.toString())
        println("Все логические планы записаны в $allPath")

        val best = rootG.bestExpression
        println("best expression: $best")

        println("Оценка кандидатов в группе ${rootG.tables}:")
        for (expr in rootG.expressions) {
            val cost = CostModel.cost(expr.op, expr.children)
            println("- ${expr.op} : cost=$cost")
        }

        if (best == null) {
            println("План не найден")
            continue
        }


        val outPath = Paths.get("build", "plans").apply { Files.createDirectories(this) }
            .resolve("plan-$ts.txt")
        println("Лучший логический план записан в $outPath")

        println("OK: найдено ${rootG.expressions.size} эквивалент(а/ов), стоимость = ${rootG.bestCost}")

        val resultDir = Paths.get("build", "output").apply { Files.createDirectories(this) }
        val resultFile = resultDir.resolve("result-$ts.txt")
        println("Выполняем физический план и пишем результат в: $resultFile")

        val executor = Executor(
            tablePaths = tablePaths,
            schemas = schemasMap
        )

        //val time = measureTime { executor.executePhysicalPlanToFile(best, resultFile) }

        val topExecutor = TopPlansExecutor(rootG, executor)
        val topN = 6
        val results = topExecutor.runTopPlans(topN = topN)

        val bestResult = results.minByOrNull { it.cost }!!
        println("ЛУЧШИЙ план из $topN: стоимость = ${bestResult.cost}, время = ${bestResult.duration}")


        println("Выполнение завершено, результат доступен в $resultFile")
        //println("Время выполнения $time")
        TimingStats.print()
    }
}

private fun printHelp() = println(
    """
    Доступные команды:
      help         – показать эту справку
      exit         – выйти из программы

    Формат запроса JOIN (без SELECT/WHERE):
      <table1> <JOIN_TYPE> <table2> ON <table1.col><op><table2.col> 
           [ <JOIN_TYPE> <table3> ON <table1/2.col><op><table3.col> ] ...

    JOIN_TYPE:
      INNER | LEFT | RIGHT | FULL | SEMI | ANTI | ASOF

    Операторы условий:
      =   <>   <   <=   >   >=

    Примеры:
      A INNER B ON A.id = B.id
      A LEFT  B ON A.key = B.key   RIGHT  C ON B.key = C.key
      Orders INNER Customers ON Orders.cust = Customers.id
           INNER Items ON Orders.item_id < Items.id

    После ввода запроса будет создан логический план, 
    оптимизирован и сохранён в build/plans/plan-<timestamp>.txt
    """.trimIndent()
)