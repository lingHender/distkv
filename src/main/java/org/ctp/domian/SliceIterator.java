package org.ctp.domian;

import javafx.util.Pair;

import java.io.IOException;
import java.util.Iterator;

public class SliceIterator implements Iterator<Pair<String, String>>{
    private final SliceStream sliceStream;

    public SliceIterator(Slice slice) {
        sliceStream = new SliceStream(slice);
    }

    @Override
    public boolean hasNext() {
        try {
            return sliceStream.available() > 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Pair<String, String> next() {
        try {
            int keyLength = sliceStream.readInt();
            int valueLength = sliceStream.readInt();

            String key = sliceStream.read(keyLength);
            String value = null;

            if (valueLength != 0) {
                 value = sliceStream.read(valueLength);
            }

            return new Pair<>(key, value);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
