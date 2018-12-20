package org.ctp.service;

import com.google.common.hash.BloomFilter;
import javafx.util.Pair;
import org.ctp.contants.SSTableContants;
import org.ctp.domian.BlockIndex;
import org.ctp.domian.Slice;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lfli on 25/07/2018.
 */
public class WriteService {
    private static DataOutputStream ioStream;
    private long blockSize = 0;
    private long blockStart = 0;

    private final List<BlockIndex> blockIndices = new ArrayList<>();
    private long indexOffset = 0;
    private long bloomFilterOffset = 0;

    public WriteService(OutputStream ioStream) {
        this.ioStream = new DataOutputStream(ioStream);
    }

    public void writeEntry(String key, String value) throws IOException {
        System.out.println("write key-value:" + key + "," + value);
        ioStream.writeInt(key.length());
        ioStream.writeInt(value.length());
        ioStream.write(key.getBytes());
        if (value.length() > 0) {
            ioStream.write(value.getBytes());
        }

        blockSize++;

        if(blockSize > SSTableContants.BLOCK_RECORD_SIZE) {
            closeDataBlock();
        }
    }

    public void writeEntry(Pair<String, String> entity) throws IOException{
        writeEntry(entity.getKey(), entity.getValue());
    }


    public void closeDataBlock() {
        System.out.println("finish one data block");
        if (blockSize > 0) {
            long size = ioStream.size() - blockStart;
            BlockIndex blockIndex = new BlockIndex(blockStart, size);
            blockIndices.add(blockIndex);
        }

        blockStart = ioStream.size();
        blockSize = 0;
    }

    public void writeBlockIndex() throws IOException {
        indexOffset = ioStream.size();
        System.out.println("write block index:");
        for (BlockIndex blockIndex : blockIndices) {
            System.out.println("   block index offset:" + blockIndex.getOffset() + ", size:" + blockIndex.getSize());
            ioStream.writeInt((int)blockIndex.getOffset());
            ioStream.writeInt((int)blockIndex.getSize());
        }
    }

    public void writeBloomFilter(BloomFilter<String> filter) throws IOException {
        System.out.println("write bloom filter");
        bloomFilterOffset = ioStream.size();
        System.out.println("   write bloom filter offset: " + bloomFilterOffset);
        filter.writeTo(ioStream);
    }

    public void writeFooter(int count) throws IOException {
        System.out.println("write indexOffset: " + indexOffset);
        System.out.println("write bloomFilterOffset: " + bloomFilterOffset);
        System.out.println("write records count:" + count);
        ioStream.writeInt((int)indexOffset);
        ioStream.writeInt((int)bloomFilterOffset);
        ioStream.writeInt(count);
    }

    public void close() throws IOException {
        System.out.println("close writeService");
        ioStream.close();
    }
}
