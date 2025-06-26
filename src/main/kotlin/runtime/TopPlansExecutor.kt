package org.bmstu.runtime

import cost.CostModel
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.time.Duration
import kotlin.time.measureTime
import logical.Expression
import logical.Group
import operators.PhysicalOp
import org.bmstu.execution.Executor

/**
 * Выполняет топ-N физически оптимальных планов, замеряя их стоимость и время.
 * Распечатывает полный вложенный план для каждой топ-N ветки.
 */
class TopPlansExecutor(
    private val rootGroup: Group,
    private val executor: Executor,
    private val outputDir: Path = Path.of("build", "output")
) {
    data class Result(
        val plan: Expression,
        val cost: Double,
        val duration: Duration,
        val outputPath: Path
    )

    /**
     * Рекурсивно собирает все физические Expression из дерева групп.
     */
    private fun collectAllPhysicalPlans(
        group: Group,
        acc: MutableList<Expression>
    ) {
        for (expr in group.expressions) {
            if (expr.op is PhysicalOp) {
                acc += expr
            }
            for (childGroup in expr.children) {
                collectAllPhysicalPlans(childGroup, acc)
            }
        }
    }

    private fun collectRootPhysicalPlans(root: Group): List<Expression> =
        root.expressions.filter { it.op is PhysicalOp }

    /**
     * Рекурсивно строит отступами текстовое представление плана, начиная с expr.
     */
    private fun dumpPlan(expr: Expression, indent: String = ""): StringBuilder {
        val sb = StringBuilder()
        sb.append(indent)
            .append("+─ ")
            .append(expr.op.javaClass.simpleName)
            .append(" [")
            .append(expr.op)
            .append("]")
            .appendLine()

        // для каждого дочернего входа в expr берём его лучший физический под-план
        for (childGroup in expr.children) {
            // тут предполагаем, что в childGroup.bestExpr хранится нужный исполнительный Expression
            val childExpr = childGroup.bestExpression
            if (childExpr != null && childExpr.op is PhysicalOp) {
                sb.append(dumpPlan(childExpr, indent + "   "))
            }
        }
        return sb
    }

    /**
     * Запускает топ-N планов, сохраняет результаты и возвращает метрики.
     */
    fun runTopPlans(topN: Int = 3): List<Result> {
        val timestamp = LocalDateTime.now()
            .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
        Files.createDirectories(outputDir)

        // 1) Берём только корневые физические планы
        val rootPlans = collectRootPhysicalPlans(rootGroup)
        if (rootPlans.isEmpty()) {
            println("Нет физических планов для выполнения.")
            return emptyList()
        }

        // 2) Чтобы не исполнить один и тот же план несколько раз —
        //    сравниваем по «печати» плана
        val uniquePlans = rootPlans
            .distinctBy { dumpPlan(it).toString() }

        // 3) Сортируем по оценке стоимости и берём top-N
        val exprsWithCost = uniquePlans
            .map { it to CostModel.cost(it.op, it.children) }
            .sortedBy { it.second }
            .take(topN)

        val statsFile = outputDir.resolve("top-plans-$timestamp.txt")
        val stats = StringBuilder()
        val results = mutableListOf<Result>()

        exprsWithCost.forEachIndexed { idx, (expr, cost) ->
            val outFile = outputDir.resolve("plan${idx + 1}-$timestamp.txt")

            println("→ Выполняем план #${idx + 1}, стоимость = $cost")
            val duration = measureTime {
                executor.executePhysicalPlanToFile(expr, outFile)
            }
            println("✓ План #${idx + 1} выполнен за $duration, результат: $outFile")

            stats.appendLine("=== План #${idx + 1} ===")
            stats.appendLine(dumpPlan(expr).toString())
            stats.appendLine("Стоимость: $cost")
            stats.appendLine("Время выполнения: $duration")
            stats.appendLine("Результат записан в: $outFile")
            stats.appendLine()

            results += Result(expr, cost, duration, outFile)
        }

        Files.writeString(statsFile, stats.toString())
        println("Результаты выполнения топ-$topN планов записаны в $statsFile")
        return results
    }

}
