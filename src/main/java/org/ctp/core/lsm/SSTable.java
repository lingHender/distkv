package org.ctp.core.lsm;

import com.google.common.hash.BloomFilter;
import org.ctp.bloomfilter.StringFunnel;
import org.ctp.contants.SSTableContants;
import org.ctp.service.CommonService;
import org.ctp.service.MergeSSTables;
import org.ctp.service.ReadService;
import org.ctp.service.WriteService;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static org.ctp.service.CommonService.getFilesUnderDirectory;
import static org.ctp.service.CommonService.sortedRandomAccessFiles;

public class SSTable implements Runnable{
    private Thread t;
    private final String THREAD_NAME ="Write to SSTables Thread";
    private MemTable memTable;

    public void start() {
            t = new Thread(this, THREAD_NAME);
            t.start();
    }

    @Override
    public void run() {
        try {
            String ssTableName = System.currentTimeMillis() + ".sst";
            System.out.println(" SStables file Name:" + ssTableName);
            writeTo(ssTableName);

            if(reachSStablesLimition()) {
                mergeSStable();
            }

        } catch (Exception e) {
            System.out.println("ops! Error happened when write to SSTable or merge SStables");
            System.out.println(e.getStackTrace());
        }
    }

    public String readFrom(String key) {
        ArrayList<File> files = getFilesUnderDirectory(SSTableContants.RECORD_PATH);
        List<RandomAccessFile> randomAccessFiles = sortedRandomAccessFiles(files);
        ReadService readService = new ReadService(key);
        String value;
        try {
            for (int i=0; i < randomAccessFiles.size(); i++) {
                value = readService.read(randomAccessFiles.get(i));
                if(value != null || value != "-") {
                    return value;
                }
            }

        } catch (IOException e) {
            System.out.println(e.getStackTrace());
        }

        return null;
    }

    public void writeTo(String ssTableName) {
        try {
            File file = new File(SSTableContants.RECORD_PATH, ssTableName);
            if (!file.exists()) {
                file.createNewFile();
            }

            WriteService writeService = new WriteService(new FileOutputStream(file));
            BloomFilter<String> bloomFilter = BloomFilter.create(StringFunnel.getInstance(), memTable.getMap().size());

            for (Entry<String, String> entry : memTable.getMap().entrySet()) {
                writeService.writeEntry(entry.getKey(), entry.getValue());
                bloomFilter.put(entry.getKey());
            }

            writeService.closeDataBlock();
            writeService.writeBlockIndex();
            writeService.writeBloomFilter(bloomFilter);
            writeService.writeFooter(memTable.getMap().size());
            writeService.close();

        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
    }

    public void setMemTable(MemTable memTable) {
        this.memTable = memTable;
    }

    private boolean reachSStablesLimition (){
        List<File> files = getFilesUnderDirectory(SSTableContants.RECORD_PATH);
        return files.size() >= SSTableContants.SSTABLES_LIMIT;
    }

    private void mergeSStable() {
        String mergedSStablesName = System.currentTimeMillis() + ".sst";
        System.out.println("Merged SStables file Name:" + mergedSStablesName);

        MergeSSTables mergeSSTables = new MergeSSTables();
        mergeSSTables.setMergedSSTableName(mergedSStablesName);
        mergeSSTables.setFiles(CommonService.getFilesUnderDirectory(SSTableContants.RECORD_PATH));
        mergeSSTables.start();
    }

}
