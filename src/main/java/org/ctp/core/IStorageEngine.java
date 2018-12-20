package org.ctp.core;

import java.io.Closeable;

public interface IStorageEngine extends Closeable {
    void initEngine(String logFile);
    boolean put(String key, String value);
    String read(String key);
    boolean delete(String key);
    void compareAndSet(String key, String newValue);
}
