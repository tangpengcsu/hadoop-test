package com.szkingdom;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @DESCRIPTION ${DESCRIPTION}
 * @Author TangPeng
 * @Date 2017-3-8
 */
public class UploadThread implements Runnable {

    private FileSystem fs;
    private Path srcPath;
    private Path outPath;

    public UploadThread(FileSystem fs, Path srcPath, Path outPath) {
        this.fs = fs;
        this.srcPath = srcPath;
        this.outPath = outPath;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(500);
            System.out.println("正在运行线程："+Thread.currentThread().getName());
            long start=System.currentTimeMillis();
            fs.copyFromLocalFile(srcPath, outPath);
            long end =System.currentTimeMillis();
            System.out.println("上传 "+ srcPath.getName()+" 用时："+(end-start)/1000+" s");
        }catch (InterruptedException ie){
            ie.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
