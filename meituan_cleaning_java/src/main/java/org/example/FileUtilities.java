package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileUtilities {

    private static FileSystem fileSystem = null;

    public static void checkFileExists(Configuration configuration, Path path) throws IOException {

        fileSystem = FileSystem.get(configuration);

        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
            System.out.println("文件已存在，已删除: " + path.toString());
        }

        if(null != fileSystem){
            fileSystem.close();
        }
    }



}
