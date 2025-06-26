package org.bmstu.joins.algorithms

import org.bmstu.tables.Row

interface TupleIterator {
    fun open()                // инициализация (открываем входные итераторы, выделяем буферы)
    fun next(): Row?          // возвращает следующий кортеж или null, когда конец
    fun close()               // освобождаем ресурсы
}