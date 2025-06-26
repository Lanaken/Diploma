package org.bmstu.joins

enum class JoinCommand {
    INNER,
    LEFT,
    RIGHT,
    FULL;

    companion object {
        fun parse(input: String): JoinCommand {
            return entries.firstOrNull { it.name.equals(input, ignoreCase = true) }
                ?: throw IllegalArgumentException("Unknown join command: $input")
        }
    }
}

