package org.ctp.service;

import com.google.common.hash.BloomFilter;
import javafx.util.Pair;
import org.ctp.bloomfilter.StringFunnel;
import org.ctp.contants.SSTableContants;
import org.ctp.domian.Slice;
import org.ctp.domian.SliceIterator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.ctp.service.CommonService.sortedRandomAccessFiles;

/**
 * Created by lfli on 17/08/2018.
 */
public class MergeSSTables implements Runnable{
    private Thread t;
    private final String THREAD_NAME = "Merge SSTables Thread";
    private ArrayList<File> files;
    private String mergedSSTableName;
    BloomFilter<String> bloomFilter;

    public void start() {
            t = new Thread(this, THREAD_NAME);
            t.start();
    }

    @Override
    public void run(){
        List<RandomAccessFile> ssTables = sortedRandomAccessFiles(files);
        try{
            System.out.println("Start merge SSTables");
            String latestFileName = files.get(0).getName();
            String tempMergeFileName = latestFileName.split("\\.")[0].concat("_M.sst");
            File mergeFile = mergeSStables(ssTables, tempMergeFileName);
            for (File file : files) {
                file.delete();
            }

            mergeFile.renameTo(new File(SSTableContants.RECORD_PATH, latestFileName));

        }catch (Exception e) {
            System.out.println("ops! merge SSTables get error!");
            System.out.println(e.getStackTrace());
        }
    }

    private File mergeSStables(List<RandomAccessFile> randomAccessFiles, String tempMergeFileName) throws IOException{

        System.out.println("merge sstable file name: " + tempMergeFileName);
        File mergeFile = new File(SSTableContants.RECORD_PATH, tempMergeFileName);
        if (!mergeFile.exists()) {
            mergeFile.createNewFile();
        }

        ReadService readService = new ReadService();
        WriteService writeService = new WriteService(new FileOutputStream(mergeFile));
        List<Slice> blockDatas = new ArrayList<>();

        for (int i = 0; i < randomAccessFiles.size(); i++) {
            blockDatas.add(getBlockData(readService, randomAccessFiles.get(i)));
        }

        int totalCount = calculateAllCount(blockDatas);

        bloomFilter = BloomFilter.create(StringFunnel.getInstance(), totalCount);

        mergeBlockDatas(writeService, blockDatas);

        return mergeFile;
    }

    private int calculateAllCount(List<Slice> blockDatas) {
        int total = 0;
        for (int i = 0; i < blockDatas.size(); i++) {
            total += blockDatas.get(i).count;
        }

        return total;
    }

    private Slice getBlockData(ReadService readService,RandomAccessFile file) throws IOException{
        FileChannel channel = file.getChannel();
        int size = (int)channel.size();

        MappedByteBuffer data = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);

        int footerOffset = data.limit() - SSTableContants.FOOTER_SIZE * 3;
        int indexOffset = readService.getIndexOffset(data, footerOffset);

        int recordCount = readService.getRecordCount(data, footerOffset);

        System.out.println("A ssTable record count: " + recordCount);

        Slice blockData = new Slice(data, 0, indexOffset, recordCount);

        return blockData;

    }

    private void mergeBlockDatas(WriteService writeService, List<Slice> blockDatas) throws IOException{
        int length = blockDatas.size();
        List<Boolean> isReadNext = new ArrayList<>(length);
        List<Iterator<Pair<String, String>>> iterators = new ArrayList<>(length);
        List<Pair<String, String>> entities = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            iterators.add(new SliceIterator(blockDatas.get(i)));
            isReadNext.add(true);
            entities.add(null);
        }

        while (continueMerge(isReadNext)) {
            for (int i = 0; i < length; i++) {
                if(isReadNext.get(i) && !iterators.get(i).hasNext()) {
                    entities.set(i, null);
                    isReadNext.set(i, false);
                }
                if (isReadNext.get(i) && iterators.get(i).hasNext()) {
                    entities.set(i, iterators.get(i).next());
                }
            }

            Pair<String, String> minKey = getMinKey(entities);
            if (minKey != null) {
                System.out.println("minKey:" + minKey.getKey() + " ,value:" + minKey.getValue());
                writeToMergeFile(writeService, minKey);
            }
            int flag;

            for (int i=0; i<length; i++) {
                if (entities.get(i) != null) {
                    flag = minKey.getKey().compareTo(entities.get(i).getKey());

                    if (flag >= 0) {
                        isReadNext.set(i, true);
                    } else {
                        isReadNext.set(i, false);
                    }
                }
            }

        }

        writeService.closeDataBlock();
        writeService.writeBlockIndex();
        writeService.writeBloomFilter(bloomFilter);
        writeService.writeFooter(200);
        writeService.close();
    }

    private void writeToMergeFile(WriteService writeService, Pair<String, String> entity) throws IOException{
        if (entity.getValue() != null && entity.getValue().length() > 0 && !entity.getValue().equals("-")) {
            writeService.writeEntry(entity);
            bloomFilter.put(entity.getKey());
        }
    }

    private boolean continueMerge(List<Boolean> isReadNexts) {
        boolean result = false;
        for (Boolean isReadNext : isReadNexts ) {
            result = result || isReadNext;
        }
        return result;
    }

    public Pair<String, String> getMinKey(List<Pair<String, String>> entities) {
        Pair<String, String> minKey = null;
        for (int i = 0; i < entities.size(); i++) {
            if (entities.get(i) != null) {
                if (minKey != null) {
                    int j = minKey.getKey().compareTo(entities.get(i).getKey());
                    if (j > 0) {
                        minKey = entities.get(i);
                    }

                } else {
                    minKey = entities.get(i);
                }
            }
        }

        return minKey;
    }

    public ArrayList<File> getFiles() {
        return files;
    }

    public void setFiles(ArrayList<File> files) {
        this.files = files;
    }

    public String getMergedSSTableName() {
        return mergedSSTableName;
    }

    public void setMergedSSTableName(String mergedSSTableName) {
        this.mergedSSTableName = mergedSSTableName;
    }
}
