package rules.transformation

import logical.Expression
import logical.Memo
import operators.JoinOp
import org.bmstu.joins.JoinCommand
import rules.Rule

object RuleComm : Rule {
    override fun matches(e: Expression) =
        e.op is JoinOp && e.op.type in listOf(JoinCommand.INNER, JoinCommand.FULL)

    override fun apply(e: Expression, memo: Memo): List<Expression> {
        val j = e.op as JoinOp
        val flipped = j.copy(
            leftCol = j.rightCol,
            rightCol = j.leftCol,
            op = j.op.flipped()
        )
        val new = memo.insert(flipped, listOf(e.children[1], e.children[0]))
        return listOfNotNull(new)
    }
}