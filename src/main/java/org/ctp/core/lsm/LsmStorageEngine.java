package org.ctp.core.lsm;

import org.ctp.contants.SSTableContants;
import org.ctp.core.IStorageEngine;
import org.ctp.service.CommonService;
import org.ctp.service.MergeSSTables;

import java.io.*;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.ctp.service.CommonService.getFilesUnderDirectory;

public class LsmStorageEngine implements IStorageEngine {
    private final String ADD = "add";
    private final String READ = "read";
    private final String UPDATE = "update";
    private final String DELETE = "delete";

    private MemTable memTable;
    private MemTable immutableMemTable;

    private final Lock writeLock = new ReentrantReadWriteLock().writeLock();

    public  LsmStorageEngine() {
        this.memTable = new MemTable();
        recoveryMemTableFromLog();
    }

    @Override
    public void initEngine(String logFile) {
    }

    @Override
    public boolean put(String key, String value) {
        String record = String.format("%s|%s|%s\n", ADD, key, value);
        recordLog(record);
        int recordsLength = key.getBytes().length + value.getBytes().length;

        if (memTable.isLimited(recordsLength)) {
            writeLock.lock();
            try {
                flush();
                memTable.clear();
            } finally {
                writeLock.unlock();
            }
        }
        memTable.put(key, value);
        return true;
    }

    @Override
    public String read(String key) {
        String record = String.format("%s|%s\n", READ, key);
        recordLog(record);
        if (memTable.isContainKey(key)) {
            String value =  memTable.read(key);
            return value.equals("-") ? null : value;
        }

        SSTable ssTable = new SSTable();
        String value = ssTable.readFrom(key);

        if (value == null || value.equals("-")) {
            return null;
        }

        return value;
    }

    @Override
    public boolean delete(String key) {
        String record = String.format("%s|%s\n", DELETE, key);
        recordLog(record);

        String value = read(key);
        if (value == null || value.equals("-")) {
            return false;
        }
        memTable.put(key, "-");
        return true;
    }

    @Override
    public void compareAndSet(String key, String newValue) {
        String oldValue = read(key);
        if (oldValue.equals(null)) {
            System.out.println("There is no value for " + key);
        } else {
            String record = String.format("%s|%s|%s\n", UPDATE, oldValue, newValue);
            recordLog(record);
            put(key, newValue);
        }
    }

    @Override
    public void close() throws IOException {

    }

    public void recordLog(String record) {
        try {
            File logFile = new File(SSTableContants.LOG_PATH, "records.wal");

            if (!logFile.exists()) {
                logFile.createNewFile();
            }

            FileWriter writer = new FileWriter(logFile, true);
            writer.append(record);
            writer.close();
        } catch (IOException e) {
            System.out.println(e.getStackTrace());
        }
    }

    public void flush() {
        writeLock.lock();
        try {
            File logFile = new File(SSTableContants.LOG_PATH, "records.wal");
            SSTable ssTable = new SSTable();
            immutableMemTable = memTable;
            ssTable.setMemTable(immutableMemTable);
            ssTable.start();
            logFile.delete();
        } finally {
            writeLock.unlock();
        }
    }

    private void recoveryMemTableFromLog() {
        File file = new File(SSTableContants.LOG_PATH, "records.wal");
        if (file.exists()) {
            writeLock.lock();
            try {
                Scanner scanner = new Scanner(file);
                while (scanner.hasNextLine()) {
                    String[] commands = scanner.nextLine().split("|");

                    
                }
            } catch (Exception e) {
                System.out.println("ops! recovery log failed");
                System.out.println(e.getStackTrace());
            } finally {

            }
        }
    }

}
