package org.bmstu.indexes

//class BPlusTreeIndex<K : Comparable<K>>(
//    private val order: Int = 64 // max children per internal node
//) : Index<K> {
//
//    private var root: Node = LeafNode()
//
//    override fun insert(key: K, rid: Rid) {
//        val split = root.insert(key, rid)
//        if (split != null) {
//            val (pivot, sibling) = split
//            val newRoot = InternalNode()
//            newRoot.keys.add(pivot)
//            newRoot.children.add(root)
//            newRoot.children.add(sibling)
//            root = newRoot
//        }
//    }
//
//    override fun seekEqual(key: K): List<Rid> = root.seekEqual(key)
//
//    override fun seekRange(range: KeyRange<K>): Iterator<Pair<K, List<Rid>>> = root.seekRange(range)
//
//    // ---- Node hierarchy ----
//    private abstract inner class Node {
//        abstract fun insert(key: K, rid: Rid): SplitResult?
//        abstract fun seekEqual(key: K): List<Rid>
//        abstract fun seekRange(range: KeyRange<K>): Iterator<Pair<K, List<Rid>>>
//    }
//
//    private inner class InternalNode : Node() {
//        val keys = mutableListOf<K>()
//        val children = mutableListOf<Node>()
//
//        override fun insert(key: K, rid: Rid): SplitResult? {
//            val i = keys.binarySearch(key).let { if (it >= 0) it + 1 else -it - 1 }
//            val split = children[i].insert(key, rid)
//            if (split != null) {
//                val (newKey, newChild) = split
//                keys.add(i, newKey)
//                children.add(i + 1, newChild)
//                if (keys.size >= order) return splitInternal()
//            }
//            return null
//        }
//
//        private fun splitInternal(): SplitResult {
//            val mid = keys.size / 2
//            val sibling = InternalNode()
//            sibling.keys.addAll(keys.subList(mid + 1, keys.size))
//            sibling.children.addAll(children.subList(mid + 1, children.size))
//
//            val upKey = keys[mid]
//
//            keys.subList(mid, keys.size).clear()
//            children.subList(mid + 1, children.size).clear()
//
//            return SplitResult(upKey, sibling)
//        }
//
//        override fun seekEqual(key: K): List<Rid> {
//            val i = keys.binarySearch(key).let { if (it >= 0) it + 1 else -it - 1 }
//            return children[i].seekEqual(key)
//        }
//
//        override fun seekRange(range: KeyRange<K>): Iterator<Pair<K, List<Rid>>> {
//            val i = keys.indexOfFirst { range.lower?.let { l -> it >= l } ?: true }.coerceAtLeast(0)
//            return children[i].seekRange(range)
//        }
//    }
//
//    private inner class LeafNode : Node() {
//        val entries = sortedMapOf<K, MutableList<Rid>>()
//        var next: LeafNode? = null
//
//        override fun insert(key: K, rid: Rid): SplitResult? {
//            val list = entries.getOrPut(key) { mutableListOf() }
//            list.add(rid)
//            if (entries.size >= order) return splitLeaf()
//            return null
//        }
//
//        private fun splitLeaf(): SplitResult {
//            val midKey = entries.keys.elementAt(entries.size / 2)
//            val sibling = LeafNode()
//            val tail = entries.tailMap(midKey).toSortedMap()
//            sibling.entries.putAll(tail)
//            tail.keys.forEach { entries.remove(it) }
//
//            sibling.next = this.next
//            this.next = sibling
//
//            return SplitResult(midKey, sibling)
//        }
//
//        override fun seekEqual(key: K): List<Rid> = entries[key]?.toList() ?: emptyList()
//
//        override fun seekRange(range: KeyRange<K>): Iterator<Pair<K, List<Rid>>> {
//            val result = mutableListOf<Pair<K, List<Rid>>>()
//            var node: LeafNode? = this
//            while (node != null) {
//                for ((k, v) in node.entries) {
//                    if (range.contains(k)) result.add(k to v.toList())
//                    else if (range.upper != null && k > range.upper) return result.iterator()
//                }
//                node = node.next
//            }
//            return result.iterator()
//        }
//    }
//
//    private data class SplitResult(val key: K, val sibling: Node)
//}