package reader

@JvmInline
value class Rid(val pos: Long) {
    companion object {
        val NULL = Rid(-1L)
    }
}


