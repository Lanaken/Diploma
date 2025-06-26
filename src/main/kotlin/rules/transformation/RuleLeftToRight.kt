package rules.transformation

import logical.Expression
import logical.Memo
import operators.JoinOp
import org.bmstu.joins.JoinCommand
import rules.Rule

object RuleLeftToRight : Rule {
    override fun matches(e: Expression) = e.op is JoinOp && e.op.type == JoinCommand.LEFT
    override fun apply(e: Expression, memo: Memo): List<Expression> {
        val j = e.op as JoinOp
        val newOp = j.copy(
            type = JoinCommand.RIGHT,
            leftCol = j.rightCol,
            rightCol = j.leftCol,
            op = j.op.flipped()
        )
        val new = memo.insert(newOp, listOf(e.children[1], e.children[0]))
        return listOfNotNull(new)
    }
}
