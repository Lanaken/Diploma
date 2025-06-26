package logical

import rules.Rule

class Engine(
    val memo: Memo,
    val logicalRules: List<Rule>,
    val physicalRules: List<Rule>
) {
    val jobs: ArrayDeque<Job> = ArrayDeque()
    fun run() {
        while (jobs.isNotEmpty()) {
            val job = jobs.removeLast()
            job.run(this)
        }
    }
}