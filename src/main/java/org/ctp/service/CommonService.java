package org.ctp.service;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by lfli on 17/08/2018.
 */
public class CommonService {

    public static ArrayList<File> getFilesUnderDirectory(String path) {
        ArrayList<File> fList = new ArrayList<>();
        File directory = new File(path);
        File[] files = directory.listFiles();
        for (File file : files){
            if (file.isFile()){
                fList.add(file);
            }
        }

        return fList;
    }

    public static List<RandomAccessFile> sortedRandomAccessFiles(ArrayList<File> fList) {
        ArrayList<String> sortedFile = new ArrayList<>();
        List<RandomAccessFile> sortedRandomFile = new ArrayList<>();

        for (File file : fList) {
            if (file.isFile()){
                sortedFile.add(file.getAbsoluteFile().toString());
            }
        }

        Collections.sort(sortedFile, Collections.reverseOrder());

        try {
            for(String fileName : sortedFile) {
                sortedRandomFile.add(new RandomAccessFile(fileName, "r"));
            }
        } catch (Exception e) {
            System.out.println("error happened");
        }

        return sortedRandomFile;
    }
}
