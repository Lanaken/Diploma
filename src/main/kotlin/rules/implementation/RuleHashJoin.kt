import logical.Expression
import logical.Group
import logical.Memo
import operators.HashJoinOp
import operators.JoinOp
import org.bmstu.joins.JoinCommand
import org.bmstu.util.MemoryUtil
import rules.Rule
import util.isEquality

object RuleHashJoin : Rule {
    override fun matches(expr: Expression): Boolean {
        val j = expr.op as? JoinOp ?: return false
        return isEquality(j.op) && j.type in setOf(
            JoinCommand.INNER,
            JoinCommand.LEFT,
            JoinCommand.RIGHT,
            JoinCommand.FULL
        )
    }

    override fun apply(expr: Expression, memo: Memo): List<Expression> {
        val j = expr.op as JoinOp
        val leftGroup = expr.children[0]
        val rightGroup = expr.children[1]
        val results = mutableListOf<Expression>()

        fun sizeOf(g: Group): Long = (g.bestCard * g.avgRowSize).toLong()
        val memThreshold = (MemoryUtil.freeHeap() * 0.4).toLong()
        val leftSizeBytes = sizeOf(leftGroup)
        val rightSizeBytes = sizeOf(rightGroup)
        val leftFits = leftSizeBytes < memThreshold
        val rightFits = rightSizeBytes < memThreshold

        if (leftFits) {
            memo.insert(
                HashJoinOp(
                    joinType = j.type,
                    outerKey = j.leftCol,
                    innerKey = j.rightCol,
                    buildSideLeft = true,
                    partitioned = false
                ),
                listOf(leftGroup, rightGroup)
            )?.let { results += it }
        }

        else {
            memo.insert(
                HashJoinOp(
                    joinType = j.type,
                    outerKey = j.leftCol,
                    innerKey = j.rightCol,
                    buildSideLeft = true,
                    partitioned = true
                ),
                listOf(leftGroup, rightGroup)
            )?.let { results += it }
        }

//        if (rightFits) {
//            memo.insert(
//                HashJoinOp(
//                    joinType = j.type,
//                    outerKey = j.rightCol,
//                    innerKey = j.leftCol,
//                    buildSideLeft = false,
//                    partitioned = false
//                ),
//                listOf(rightGroup, leftGroup)
//            )?.let { results += it }
//        } else {
//            memo.insert(
//                HashJoinOp(
//                    joinType = j.type,
//                    outerKey = j.leftCol,
//                    innerKey = j.rightCol,
//                    buildSideLeft = false,
//                    partitioned = true
//                ),
//                listOf(rightGroup, leftGroup)
//            )?.let { results += it }
//        }

        return results
    }
}

