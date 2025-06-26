package org.bmstu.reader

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.net.URLDecoder
import java.nio.file.Files
import java.nio.file.Paths
import java.util.zip.ZipFile

class SchemasLoader {
    companion object {
        fun listResourceDir(dir: String): List<String> {
            val cl = Thread.currentThread().contextClassLoader
            val url = cl.getResource(dir) ?: error("Каталог $dir не найден")

            return when (url.protocol) {
                "file" -> {
                    val path = Paths.get(url.toURI())
                    Files.list(path)
                        .filter { Files.isRegularFile(it) }
                        .map { it.fileName.toString() }
                        .toList()
                }

                "jar" -> {
                    val jarPath = url.path.substringAfter("file:").substringBefore("!")
                    val decodedJarPath = URLDecoder.decode(jarPath, "UTF-8")
                    ZipFile(decodedJarPath).use { jar ->
                        jar.entries().asSequence()
                            .filter { it.name.startsWith(dir) && !it.isDirectory }
                            .map { it.name.removePrefix("$dir/") }
                            .toList()
                    }
                }

                else -> error("Неизвестный протокол ${url.protocol}")
            }
        }

        fun loadSchema(resourcePath: String): YSchema {
            val cl = Thread.currentThread().contextClassLoader
            val url = cl.getResource(resourcePath) ?: error("Файл $resourcePath не найден")

            val yamlMapper = YAMLMapper().registerKotlinModule()

            val schema = when (url.protocol) {
                "file" -> {
                    val path = Paths.get(url.toURI())
                    yamlMapper.readValue(path.toFile(), YSchema::class.java)
                }
                "jar" -> {
                    cl.getResourceAsStream(resourcePath).use { input ->
                        yamlMapper.readValue(input, YSchema::class.java)
                    }
                }
                else -> error("Неизвестный протокол ${url.protocol}")
            }

            val fixedColumns = schema.columns.map { col ->
                if (col.type == CType.STRING && col.length == null)
                    col.copy(length = 256)
                else col
            }
            val normalizedSchema = schema.copy(columns = fixedColumns)

            validateSchema(normalizedSchema)
            return normalizedSchema
        }


        private fun validateSchema(schema: YSchema) {
            schema.columns.forEach { col ->
                when (col.type) {
                    CType.DECIMAL -> {
                        require(col.precision != null) { "DECIMAL должен иметь precision: ${col.name}" }
                        require(col.scale != null) { "DECIMAL должен иметь scale: ${col.name}" }
                        require(col.length == null) { "DECIMAL не должен иметь length: ${col.name}" }
                    }

                    CType.CHAR, CType.STRING -> {
                        require(col.length != null) { "CHAR должен иметь length: ${col.name}" }
                        require(col.precision == null) { "CHAR не должен иметь precision: ${col.name}" }
                        require(col.scale == null) { "CHAR не должен иметь scale: ${col.name}" }
                    }

                    CType.INT, CType.BIGINT, CType.DATE -> {
                        require(col.length == null) { "${col.type} не должен иметь length: ${col.name}" }
                        require(col.precision == null) { "${col.type} не должен иметь precision: ${col.name}" }
                        require(col.scale == null) { "${col.type} не должен иметь scale: ${col.name}" }
                    }
                }
            }
        }
    }
}