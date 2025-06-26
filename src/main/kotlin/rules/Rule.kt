package rules

import logical.Expression
import logical.Memo

interface Rule {
    fun matches(e: Expression): Boolean
    fun apply(e: Expression, memo: Memo): List<Expression>      // может породить >1 Expr
}