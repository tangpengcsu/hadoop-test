package com.szkingdom;

/**
 * @DESCRIPTION ${DESCRIPTION}
 * @Author TangPeng
 * @Date 2017-3-7
 */
public class Main {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        int len = args.length;
        String srcPath = null;
        String hdfsURL = null;
        String dstPath = null;
        String fileType = null;
        if (args.length < 1) {
            srcPath = "D:\\workspace\\IdeaProjects\\hadoop-test\\*";
            dstPath = "hdfs://192.168.200.30:9000/data/test3";
            fileType = "txt";
        } else if (len > 4) {
            System.out.println("输入过多无效参数");
            System.exit(0);
        } else {
            //指定在元数据目录的地址在linux环境下
            srcPath = args[0] + "*";
            if (len == 4 && !(args[3] == null || args[3].equals(""))) {
                hdfsURL = args[3];
            } else {
                hdfsURL = "hdfs://192.168.200.30:9000/";
            }
            dstPath = hdfsURL + args[1];
            fileType = args[2];
        }
        int cNum = 0;
    /*    while (cNum<1) {
            System.out.println(cNum++);
            new Thread(new UploadThread(srcPath, hdfsURL, dstPath, fileType)).start();
        }*/
        new FileUpload(srcPath, hdfsURL, dstPath, fileType).upload();
    }
}
