package util

import net.jpountz.xxhash.XXHashFactory

object HashUtil {
    private val factory = XXHashFactory.fastestInstance()
    private val seed = 0 // может быть любым

    fun xxHash32(input: ByteArray): Int {
        val hasher = factory.hash32()
        return hasher.hash(input, 0, input.size, seed)
    }
}