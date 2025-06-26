package org.bmstu.indexes

import reader.Rid

interface Index<K : Comparable<K>> {

    fun seekEqual(key: K): List<Rid>

    /** Range seek – iterator of <key,List<Rid>> in **ascending** key order. */
    fun seekRange(range: KeyRange<K>): Iterator<Pair<K, List<Rid>>>
}