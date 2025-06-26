package logical

import cost.CostModel
import cost.CostModel.cost
import operators.PhysicalOp
import operators.ScanOp
import statistic.Catalog
import statistic.synthesizeJoinStats

sealed interface Job { fun run(s: Engine) }

class ExploreGroupJob(private val grp: Group) : Job {
    override fun run(s: Engine) {
        if (grp.explored) return
        grp.explored = true
        grp.expressions.forEach { s.jobs += ExploreExprJob(it) }
    }
}

class ExploreExprJob(private val expr: Expression) : Job {
    override fun run(s: Engine) {
        for (r in s.logicalRules) {
            if (r !in expr.appliedRules && r.matches(expr)) {
                expr.appliedRules += r
                val newExprs = r.apply(expr, s.memo)
                newExprs.forEach { ne ->
                    val g = s.memo.groupOf(ne.tables)
                    s.jobs += ExploreGroupJob(g)
                    s.jobs += ExploreExprJob(ne)
                }
            }
        }
        s.jobs += ImplementExprJob(expr)
    }
}

class ImplementExprJob(private val expr: Expression) : Job {
    override fun run(s: Engine) {
        for (r in s.physicalRules) {
            if (r !in expr.appliedRules && r.matches(expr)) {
                expr.appliedRules += r
                val newExprs = r.apply(expr, s.memo)
                newExprs.forEach { newExpr ->
                    s.memo.groupOf(newExpr.tables)
                    s.jobs += ExploreExprJob(newExpr)
                    s.jobs += OptInputsJob(newExpr, 0, Double.POSITIVE_INFINITY)
                }
            }
        }
    }
}


class OptimizeGroupJob(private val grp: Group) : Job {
    override fun run(s: Engine) {

        /* 1. Если группа ещё не исследована – сначала Explore */
        if (!grp.explored) {
            s.jobs += this
            s.jobs += ExploreGroupJob(grp)
            return
        }
        if (grp.bestExpression != null) return      // уже посчитали

        var bestCost = Double.POSITIVE_INFINITY
        var bestExpr: Expression? = null
        var bestCard = 1.0
        var bestSize = 0L

        /* 2. оцениваем все выражения группы */
        for (expr in grp.expressions) {

            /* рекурсивно оптимизируем детей */
            expr.children.forEach { s.jobs += OptimizeGroupJob(it) }

            /* ― row-size для текущего Expr ― */
            bestSize = when (expr.op) {
                is ScanOp -> Catalog.stats(expr.op.table).avgRowSize.toLong()
                is PhysicalOp -> expr.children.sumOf { it.avgRowSize }
                else -> continue
            }

            /* ― стоимость ― */
            val cost = CostModel.cost(expr.op, expr.children)
            if (cost < bestCost) {
                bestCost = cost
                bestExpr = expr
                bestCard = CostModel.estimateCardinality(expr.op, expr.children)
            }
        }

        /* 3. финализируем лучшую альтернативу */
        grp.bestCost = bestCost
        grp.bestExpression = bestExpr
        grp.bestCard = bestCard
        grp.avgRowSize = bestSize

        /* 4. синтетическая статистика для результатов JOIN-ов */
        bestExpr?.let { be ->
            if (be.children.size == 2) {                     // любой бинарный оператор
                val leftStats = Catalog.stats(be.children[0].tables)
                val rightStats = Catalog.stats(be.children[1].tables)
                val joinStats = synthesizeJoinStats(leftStats, rightStats, bestCard.toLong())
                Catalog.putStats(grp.tables, joinStats)
            }
        }
    }
}

class OptInputsJob(
    private val expr: Expression,
    private val nextChild: Int,
    private val limit: Double
) : Job {
    override fun run(s: Engine) {
        if (nextChild < expr.children.size) {
            val childGrp = expr.children[nextChild]
            val childExpr = childGrp.bestExpression ?: run {
                s.jobs += this
                s.jobs += OptimizeGroupJob(childGrp)
                return
            }

            val newLimit = updateCostLimit(childExpr, limit)
            if (newLimit <= 0.0) return

            s.jobs += OptInputsJob(expr, nextChild + 1, newLimit)
        } else {
            val grp = s.memo.groupOf(expr.tables)
            val cost = cost(expr.op, expr.children)

            if (limit <= 0.0 || cost > limit) return

            if (cost < grp.bestCost) {
                grp.bestCost = cost
                grp.bestExpression = expr
                grp.bestCard = CostModel.estimateCardinality(expr.op, expr.children)
                grp.avgRowSize = when (expr.op) {
                    is ScanOp -> Catalog.stats((expr.op as ScanOp).table).avgRowSize.toLong()
                    is PhysicalOp -> expr.children.sumOf { it.avgRowSize }
                    else -> 0L
                }
            }
        }
    }
}

private fun updateCostLimit(expr: Expression, limit: Double): Double {
    val est = cost(expr.op, expr.children)
    return if (limit < 0) limit else limit - est
}
