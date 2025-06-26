package org.bmstu.util

class UniqueSetByField<T>() {
    private val set: HashSet<T> = HashSet()

    fun add(elem: T) {
        if (set.contains(elem)){
            println("$elem уже существует, пропускаем")
            return
        }
        set.add(elem)
    }

    fun getAll(): HashSet<T> = set
}
