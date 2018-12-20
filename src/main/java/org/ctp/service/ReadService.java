package org.ctp.service;

import com.google.common.hash.BloomFilter;
import javafx.util.Pair;
import org.ctp.bloomfilter.StringFunnel;
import org.ctp.contants.SSTableContants;
import org.ctp.domian.Slice;
import org.ctp.domian.SliceIterator;
import org.ctp.domian.SliceStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Iterator;

public class ReadService{
    private Slice keySlice;

    public ReadService( String key) {
        this.keySlice = new Slice(key.getBytes());
    }

    public ReadService() {}

    public String read(RandomAccessFile file) throws IOException{
        FileChannel channel = file.getChannel();

        int size = (int)channel.size();

        MappedByteBuffer data = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);

        int footerOffset = data.limit() - SSTableContants.FOOTER_SIZE * 3;
        int bloomFilterOffset = getBloomFilterOffset(data, footerOffset);
        int indexOffset = getIndexOffset(data, footerOffset);
        int indexSize = bloomFilterOffset - indexOffset;
        int bloomFilterSize = footerOffset - bloomFilterOffset;


        Slice indexData = new Slice(data,indexOffset, indexSize);

        Slice blockData = getBlockByIndex(indexData, getKeyIndex(indexData));

        Slice bloomFilterData = new Slice(data,bloomFilterOffset, bloomFilterSize );

        BloomFilter<String> bloomFilter = getBloomFilter(bloomFilterData);
        String key = Charset.forName("utf-8").decode(keySlice.toByteBuffer()).toString();

        if (bloomFilter != null && !bloomFilter.mightContain(key)) {
            System.out.println("BloomFilter shows there is no this key:" + key);
            return null;
        }

        Iterator<Pair<String, String>> it = new SliceIterator(blockData);

        while (it.hasNext()) {
            Pair<String, String>  entity = it.next();

            if(key.equals(entity.getKey())) {
                return entity.getValue();
            }
        }

        return null;
    }

    public int getBloomFilterOffset(ByteBuffer data, int footerOffset) {
        ByteBuffer duplicate = data.duplicate();
        duplicate.limit(footerOffset + SSTableContants.FOOTER_SIZE * 2);
        duplicate.position(footerOffset + SSTableContants.FOOTER_SIZE);

        return duplicate.getInt();
    }

    public int getIndexOffset(ByteBuffer data, int footerOffset) {
        ByteBuffer duplicate = data.duplicate();
        duplicate.limit(footerOffset + SSTableContants.FOOTER_SIZE);
        duplicate.position(footerOffset);

        return duplicate.getInt();
    }

    public int getRecordCount(ByteBuffer data, int footerOffset) {
        ByteBuffer duplicate = data.duplicate();
        duplicate.limit(footerOffset + SSTableContants.FOOTER_SIZE * 3);
        duplicate.position(footerOffset + SSTableContants.FOOTER_SIZE * 2);

        return duplicate.getInt();
    }

    public int getKeyIndex(Slice indexData) {
        int indexSize = indexData.size / SSTableContants.INDEX_BLOCK_SIZE;
        int l = 0, r = indexSize - 1;
        Slice lKey = findBlockByIndex(indexData, l);
        Slice rKey = findBlockByIndex(indexData, r);
        int c1 = keySlice.compareTo(lKey);
        if (c1 <= 0) {
            return 0;
        }

        int c2 = keySlice.compareTo(rKey);
        if (c2 >= 0) {
            return r;
        }

        while (l < r) {
            int m = (l + r + 1) / 2;
            Slice mKey = findBlockByIndex(indexData, m);

            int c = keySlice.compareTo(mKey);
            if (c == 0) {
                return m;
            } else if (c > 0) {
                l = m;
            } else {
                r = m - 1;
            }
        }

        return l;
    }

    private Slice findBlockByIndex(Slice indexData, int blockIndex) {
        ByteBuffer duplicate = indexData.getData().duplicate();
        duplicate.position(indexData.getOffset() + blockIndex * SSTableContants.INDEX_BLOCK_SIZE);

        int blockOffset = duplicate.getInt();
        duplicate.position(blockOffset);
        int keyLength = duplicate.getInt();
        int keyOffset = blockOffset + SSTableContants.INDEX_BLOCK_SIZE;

        return new Slice(duplicate, keyOffset,keyLength);
    }

    public Slice getBlockByIndex(Slice indexData, int index) {
        ByteBuffer duplicate = indexData.getData().duplicate();
        duplicate.position(indexData.getOffset() + index * SSTableContants.INDEX_BLOCK_SIZE);

        int blockOffset = duplicate.getInt();
        int blockSize = duplicate.getInt();

        return new Slice(duplicate, blockOffset, blockSize);
    }

    private BloomFilter<String> getBloomFilter(Slice data) {
        try {

            String bloomFilter = Charset.forName("utf-8").decode(data.toByteBuffer()).toString();
            InputStream is = new ByteArrayInputStream(bloomFilter.getBytes());
            return BloomFilter.readFrom(is, StringFunnel.getInstance());
        } catch (Exception E) {
            System.out.println("ops!!! Error happened when get bloomFilter");
        }
        return null;
    }

}
