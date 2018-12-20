package org.ctp.domian;

/**
 * Created by lfli on 26/07/2018.
 */
public class BlockIndex {
    private final long offset;
    private final long size;

    public BlockIndex(long offset, long size) {
        this.offset = offset;
        this.size = size;
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }

}
