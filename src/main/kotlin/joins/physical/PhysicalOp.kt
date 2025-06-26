package org.bmstu.joins.physical

import java.nio.file.Path
import org.bmstu.reader.YSchema

interface PhysicalOp {
    fun execute(outputPath: Path): YSchema
}