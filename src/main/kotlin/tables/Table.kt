package org.bmstu.tables

class Row(val columns: HashMap<String, Any?>, val hasIndex: Boolean = false)

class Table(val name: String) {
    override fun toString(): String = name
}