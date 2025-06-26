package rules.transformation

import logical.Expression
import logical.Memo
import operators.JoinOp
import org.bmstu.joins.JoinCommand
import rules.Rule

object RuleAssoc : Rule {
    override fun matches(e: Expression): Boolean =
        e.op is JoinOp &&
                e.op.type == JoinCommand.INNER &&
                e.children[0].expressions.any {       // левый ребёнок содержит ещё один INNER
                    it.op is JoinOp && it.op.type == JoinCommand.INNER
                }

    override fun apply(expression: Expression, memo: Memo): List<Expression> {
        val topJ = expression.op as JoinOp
        val leftJoinExpr = expression.children[0].expressions
            .first { (it.op as JoinOp).type == JoinCommand.INNER }

        val a = leftJoinExpr.children[0]
        val b = leftJoinExpr.children[1]
        val c = expression.children[1]

        /* строим B⋈C */
        val bcJoin = topJ.copy(
            leftCol = (leftJoinExpr.op as JoinOp).rightCol,
            rightCol = topJ.rightCol,
            op = topJ.op.flipped()
        )
        val bcExpr = memo.insert(bcJoin, listOf(b, c)) ?: return emptyList()

        /* строим A⋈(B⋈C)  */
        val newTop = memo.insert(leftJoinExpr.op, listOf(a, memo.groupOf(bcExpr.children.flatMap { it.tables }.toSet())))

        return if (newTop == null)
            emptyList()
        else listOf(newTop)
    }
}
