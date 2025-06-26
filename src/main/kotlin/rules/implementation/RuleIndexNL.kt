package rules.implementation

import logical.Expression
import logical.Memo
import operators.IndexNLJoinOp
import operators.JoinOp
import org.bmstu.joins.JoinCommand
import statistic.Catalog
import statistic.hasIndexFor

object RuleIndexNL : rules.Rule {
    override fun matches(expr: Expression): Boolean {
        val j = expr.op as? JoinOp ?: return false
        val result = j.type in setOf(JoinCommand.INNER, JoinCommand.LEFT, JoinCommand.RIGHT)
        println("RuleIndexNL $result")
        return result
    }

    override fun apply(expr: Expression, memo: Memo): List<Expression> {
        val out = mutableListOf<Expression>()
        val j = expr.op as JoinOp
        val (leftGrp, rightGrp) = expr.children

        if (rightGrp.hasIndexFor(j.rightCol)) {
            val rightTable = rightGrp.tables.single()
            val indexPath = Catalog.getIndexPath(rightTable, j.rightCol) ?: return emptyList()
            memo.insert(
                IndexNLJoinOp(
                    joinType = j.type,
                    outerKey = j.leftCol,
                    innerKey = j.rightCol,
                    indexPath = indexPath,
                    condOp = j.op
                ),
                listOf(leftGrp, rightGrp)
            )?.let { out += it }
        }

//        if (leftGrp.hasIndexFor(j.leftCol)) {
//            val leftTable = leftGrp.tables.single()
//            val indexPath = Catalog.getIndexPath(leftTable, j.leftCol) ?: return emptyList()
//            memo.insert(
//                IndexNLJoinOp(
//                    joinType = j.type,
//                    outerKey = j.rightCol,
//                    innerKey = j.leftCol,
//                    indexPath = indexPath,
//                    condOp = j.op
//                ),
//                listOf(rightGrp,leftGrp)
//            )?.let { out += it }
//        }

        return out
    }
}
