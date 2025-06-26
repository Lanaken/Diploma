package org.bmstu.util

import java.util.Locale

object TimingStats {
    private val times = mutableMapOf<String, Long>()

    fun record(label: String, durationNs: Long) {
        times[label] = times.getOrDefault(label, 0L) + durationNs
    }

    fun print() {
        println("=== JOIN TIMING STATS ===")
        for ((label, time) in times) {
            val seconds = time / 1_000_000_000.0
            println(String.format(Locale.US, "%s: %.5f s", label, seconds))
        }
    }

    fun reset() {
        times.clear()
    }
}
