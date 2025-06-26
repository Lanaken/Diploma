package org.bmstu.reader

data class YSchema(val table: String, val columns: List<Col>)
data class Col(
    val name: String,
    val type: CType,
    val length: Int? = null,
    val precision: Int? = null,
    val scale: Int? = null,
    val index: Boolean = false
)
enum class CType { INT, BIGINT, DECIMAL, DATE, CHAR, STRING }
typealias ColParser = (String) -> Any