# Генерация и загрузка данных TPC-H

Этот проект использует утилиту `dbgen` для генерации синтетических данных в соответствии с бенчмарком [TPC-H](https://www.tpc.org/tpch/), предназначенным для оценки производительности аналитических СУБД.

---

## Требования

- `make`
- `gcc` или `clang`
- Unix-подобная ОС (Linux/macOS) или WSL (для Windows)
- (опционально) PostgreSQL или любая другая СУБД для загрузки данных

---

## Шаг 1. Клонирование и сборка `dbgen`

```bash
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make
---

## Шаг 2.
./dbgen -s 1

Появятся файлы, те что в формате .tbl нужно положить в папку tables в корне проекта
Должна появится следующие файлы:
customer.tbl
lineitem.tbl
nation.tbl
orders.tbl
part.tbl
partsupp.tbl
region.tbl
supplier.tbl
И их clean версии
