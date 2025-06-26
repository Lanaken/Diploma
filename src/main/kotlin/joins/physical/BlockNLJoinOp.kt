import java.nio.file.Files
import java.nio.file.Path
import org.bmstu.joins.ConditionOperator
import org.bmstu.joins.JoinCommand
import org.bmstu.joins.algorithms.BlockNLJoinIterator
import org.bmstu.joins.physical.PhysicalOp
import org.bmstu.reader.Col
import org.bmstu.reader.YSchema

class BlockNLJoinOp(
    private val leftOp: PhysicalOp,
    private val rightOp: PhysicalOp,
    private val leftKey: String,
    private val rightKey: String,
    private val joinType: JoinCommand,
    private val condOp: ConditionOperator
) : PhysicalOp {

    override fun execute(outputPath: Path): YSchema {
        // 1) Временные файлы, в которые запишут результат своих операторов дети:
        val tempLeftFile  = Files.createTempFile("blocknl_left_",  ".tbl")
        val tempRightFile = Files.createTempFile("blocknl_right_", ".tbl")

        // 2) Рекурсивно выполняем детей, получаем их схемы и заполняем temp-файлы:
        val leftSchema  = leftOp.execute(tempLeftFile)
        val rightSchema = rightOp.execute(tempRightFile)

        // 3) Перед тем, как запустить итератор, убедимся, что папка outputPath существует, и сам файл пустой:
        if (Files.exists(outputPath)) {
            Files.delete(outputPath)
        }
        Files.createDirectories(outputPath.parent)
        Files.createFile(outputPath)

        // 4) Запускаем BlockNLJoinIterator: он прочитает tempLeftFile + tempRightFile и сразу запишет joined‐строки в outputPath:
        val iterator = BlockNLJoinIterator(
            buildPath = tempLeftFile,
            buildSchema = leftSchema,
            probePath = tempRightFile,
            probeSchema = rightSchema,
            joinType = joinType,
            buildKey = leftKey,
            probeKey = rightKey,
            condOp = condOp,
        )
        iterator.open()
        iterator.close()

        val joinedColumns = mutableListOf<Col>()
        joinedColumns.addAll(leftSchema.columns)
        joinedColumns.addAll(rightSchema.columns)
        val joinedTableName = outputPath.fileName.toString()

        return YSchema(
            table = joinedTableName,
            columns = joinedColumns
        )
    }
}