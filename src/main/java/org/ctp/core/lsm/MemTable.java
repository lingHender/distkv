package org.ctp.core.lsm;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by lfli on 17/07/2018.
 */
public class MemTable{
    private final int LIMITATION = 1024 * 1024;

    private SortedMap<String, String> map = new ConcurrentSkipListMap<>();
    private int length = 0;

    public void put(String key, String value) {
        int recordsLength = key.getBytes().length + value.getBytes().length;
        map.put(key, value);
        length = length + recordsLength;
    }

    public boolean isContainKey(String key) {
        return map.containsKey(key);
    }

    public String read(String key) {
        return map.get(key);
    }

    public void delete(String key) {
        map.remove(key);
    }

    public boolean isLimited(int recordsLength) {
        return (length + recordsLength) >= LIMITATION;
    }

    public SortedMap<String, String> getMap() {
        return map;
    }

    public void clear() {
        this.map.clear();
        this.length = 0 ;
    }
}
