package rules.implementation

import logical.Expression
import logical.Memo
import operators.BlockNLJoinOp
import operators.JoinOp
import rules.Rule

object RuleBlockNL : Rule {
    override fun matches(expr: Expression): Boolean =
        expr.op is JoinOp

    override fun apply(expr: Expression, memo: Memo): List<Expression> {
        val j = expr.op as JoinOp
        val l = expr.children[0]
        val r = expr.children[1]
        val out = mutableListOf<Expression>()

        memo.insert(
            BlockNLJoinOp(
                j.type,
                j.leftCol,
                j.rightCol,
                true,
                j.op
            ),
            listOf(l, r)
        )?.let { out += it }

        return out
    }
}
