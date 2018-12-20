package org.ctp.domian;


import java.nio.ByteBuffer;

/**
 * Created by lfli on 19/07/2018.
 */
public class Slice {
    public  int offset;
    public  int size;
    public  int count;
    private ByteBuffer data;

    public Slice(ByteBuffer data, int offset, int size) {
        this.data = data;
        this.offset = offset;
        this.size = size;
    }

    public Slice(ByteBuffer data, int offset, int size, int count) {
        this.data = data;
        this.offset = offset;
        this.size = size;
        this.count = count;
    }

    public Slice(byte[] key) {
        ByteBuffer buffer = ByteBuffer.wrap(key);
        this.data = buffer;
        this.offset = buffer.position();
        this.size = buffer.limit() - buffer.position();
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer duplicate = data.duplicate();
        duplicate.limit(offset + size);
        duplicate.position(offset);
        return duplicate;
    }

    public int compareTo(Slice blockIndex) {
        if (this.data == blockIndex.data && this.offset == blockIndex.offset && this.size == blockIndex.size) {
            return 0;
        }

        ByteBuffer b1 = this.toByteBuffer();
        ByteBuffer b2 = blockIndex.toByteBuffer();

        int pos1 = b1.position(), r1 = b1.remaining();
        int pos2 = b2.position(), r2 = b2.remaining();

        int n = pos1 + Math.min(r1, r2) ;

        for (int i = pos1, j = pos2; i < n; i++, j++) {
            int cmp = Byte.compare(b1.get(i),b2.get(j));

            if (cmp != 0) {
                return (cmp < 0) ? -1 : 1;
            }
        }

        return (r1 == r2) ? 0 :
                ((r1 < r2) ? -1 : 1);
    }

    public int getOffset() {
        return offset;
    }

    public ByteBuffer getData() {
        return data;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
