package rules.implementation

import logical.Expression
import logical.Memo
import operators.JoinOp
import operators.SortMergeJoinOp
import org.bmstu.joins.ConditionOperator
import rules.Rule
import statistic.Catalog
import org.bmstu.util.MemoryUtil   // ← новое
import kotlin.math.ceil

object RuleSortMerge : Rule {

    override fun matches(e: Expression): Boolean {
        val j = e.op as? JoinOp ?: return false

        val lTable = e.children[0].tables.singleOrNull() ?: return false
        val rTable = e.children[1].tables.singleOrNull() ?: return false

        val lStats = Catalog.stats(lTable).col[j.leftCol]
        val rStats = Catalog.stats(rTable).col[j.rightCol]

        val alreadySorted =
            (lStats?.wasSorted == true) && (rStats?.wasSorted == true)

        val opOk = j.op != ConditionOperator.NOT_EQUALS

        val needBytes = Catalog.stats(lTable).totalBytes +
                Catalog.stats(rTable).totalBytes
        val fitsRam = MemoryUtil.isMemoryEnough(needBytes)

        val result = alreadySorted && opOk && fitsRam
        println("RuleSortMerge: sorted=$alreadySorted, opOk=$opOk, fitsRam=$fitsRam → $result")
        return result
    }

    override fun apply(e: Expression, memo: Memo): List<Expression> {
        val j = e.op as JoinOp
        val leftGrp = e.children[0]
        val rightGrp = e.children[1]

        val phys = SortMergeJoinOp(
            joinType = j.type,
            outerKey = j.leftCol,
            innerKey = j.rightCol,
            alreadySorted = true,      // ← благодаря matches()
            condOp = j.op
        )

        return listOfNotNull(memo.insert(phys, listOf(leftGrp, rightGrp)))
    }
}
