package statistic

import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import org.bmstu.indexes.BptHeader
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CatalogTest {

    private var tmpFiles = mutableListOf<Path>()

    @AfterEach
    fun tearDown() {
        tmpFiles.forEach { Files.deleteIfExists(it) }
        tmpFiles.clear()
    }

    private fun createBptFile(pageSize: Int, keyType: Byte, rootPid: Long, pagesUsed: Long): Path {
        val file = Files.createTempFile("test", ".bpt")
        tmpFiles += file.toMutableList()
        FileChannel.open(file, CREATE, READ, WRITE, TRUNCATE_EXISTING).use { ch ->
            val header = BptHeader(pageSize, keyType, rootPid, pagesUsed)
            val buf = header.toBuffer()
            buf.rewind()
            ch.write(buf, 0)
        }
        return file
    }

    @Test
    fun `loadIndexMeta computes height = 1 for small pagesUsed`() {
        val pageSize = 8192
        val pagesUsed = 10L
        val file = createBptFile(pageSize, keyType = 0, rootPid = 42, pagesUsed = pagesUsed)

        loadIndexMeta(file, table = "MyTable", col = "MyCol")
        val meta = Catalog.index("MyTable", "MyCol")!!

        assertEquals(pagesUsed, meta.leafPages, "leafPages должно совпадать с pagesUsed в заголовке")
        assertEquals(1, meta.height,           "для небольшого числа страниц высота должна быть 1")
    }

    @Test
    fun `loadIndexMeta computes height more than 1 for larger pagesUsed`() {
        val pageSize = 8192
        val pagesUsed = 20_000L
        val file = createBptFile(pageSize, keyType = 0, rootPid = 0, pagesUsed = pagesUsed)

        loadIndexMeta(file, table = "AnotherTable", col = "ColX")
        val meta = Catalog.index("AnotherTable", "ColX")!!

        assertEquals(pagesUsed, meta.leafPages)
        assertEquals(3, meta.height, "для pagesUsed=20000 высота должна быть примерно 3")
    }
}
