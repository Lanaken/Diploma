package org.bmstu.indexes

data class KeyRange<K : Comparable<K>>(
    val lower: K? = null,
    val upper: K? = null,
    val includeLower: Boolean = true,
    val includeUpper: Boolean = true
) {
    fun contains(key: K): Boolean {
        val lowerOk = lower?.let { if (includeLower) key >= it else key > it } ?: true
        val upperOk = upper?.let { if (includeUpper) key <= it else key < it } ?: true
        return lowerOk && upperOk
    }
}