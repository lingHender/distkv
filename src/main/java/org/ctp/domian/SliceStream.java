package org.ctp.domian;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by lfli on 26/07/2018.
 */
public class SliceStream extends InputStream{

    private final ByteBuffer buffer;

    public SliceStream(Slice slice) {
        this.buffer = slice.toByteBuffer();
    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    public String read(int length) {
        if (buffer.remaining() < length) {
            throw new RuntimeException();
        }

        int posLimit  = buffer.position() + length;

        ByteBuffer duplicate = buffer.duplicate();
        duplicate.limit(buffer.position() + length);

        buffer.position(posLimit);
        return  Charset.forName("utf-8").decode(duplicate).toString();

    }

    public int readInt() throws IOException {
        if (buffer.remaining() < 4) {
            throw new EOFException();
        }
        return buffer.getInt();
    }

    public int available() throws IOException {
        return buffer.remaining();
    }

}
