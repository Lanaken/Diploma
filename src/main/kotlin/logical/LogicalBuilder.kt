package logical

import operators.JoinOp
import operators.ScanOp
import org.bmstu.joins.Join
import statistic.Catalog

object LogicalBuilder {
    fun build(joins: List<Join>, memo: Memo): Group {
        val scans = mutableMapOf<String, Group>()
        fun scan(name: String): Group {
            val card = Catalog.stats(name).rows
            return scans.getOrPut(name) {
                memo.insert(ScanOp(name, card), emptyList())!!
                val group = memo.groupOf(setOf(name))
                group.bestCard = card.toDouble()
                group
            }
        }


        var cur: Group? = null
        for ((i, j) in joins.withIndex()) {
            val leftG  = cur ?: scan(j.leftTable)
            val rightG = scan(j.rightTable)
            val op = JoinOp(j.type, j.leftColumn, j.rightColumn, j.operator)
            println("→ Inserting expr with op=$op and tables=${leftG.tables + rightG.tables}")
            cur = memo.insert(op, listOf(leftG, rightG))!!
                .let { memo.groupOf(leftG.tables + rightG.tables) }
            println(cur)
            if (i == 0) scans[j.leftTable] = leftG
        }
        return cur!!
    }
}
