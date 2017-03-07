package com.szkingdom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @DESCRIPTION 批量上传文件至 HDFS
 * @Author TangPeng
 * @Date 2017-3-6
 */
public class MultipleFileUpload {
    //声明两个从不同文件系统类型的静态变量
    private static FileSystem fs = null;
    private static FileSystem local = null;
    private static String hdfsURL = "hdfs://192.168.200.30:9000/";

    private static Logger logger = Logger.getLogger(MultipleFileUpload.class);

    /*
    * 格式：hadoop jar ~/hadoop-test-1.0-SNAPSHOT-jar-with-dependencies.jar /home/server1 hadoopFile/ tgz
    * java -jar hadoop-test-1.0-SNAPSHOT-jar-with-dependencies.jar D:\\SparkData\\ test2 txt hdfs://192.168.200.30:9000/
    * java -jar hadoop-test-1.0-SNAPSHOT-jar-with-dependencies.jar D:\\workspace\\IdeaProjects\\hadoop-test\\ test2 txt hdfs://192.168.200.30:9000/
    * args[0]: 待上传文件目录，必填
    * args[1]: HDFS 目标文件目录，必填
    * args[2]: 待上传文件类型，必填
    * args[4]: hdfs 地址，可填
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int len = args.length;
        String srcPath = "";
        String dstPath = "";
        String fileType = "";
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
            }
            dstPath = hdfsURL + args[1];
            fileType = args[2];
        }
        System.out.println("待上传文件目录：" + srcPath);
        System.out.println("HDFS 目标文件目录" + dstPath);
        //上传到 HDFS
        upload(srcPath, dstPath, fileType);
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("It takes " + (float) (endTime - startTime) / 1000 + "s");
    }

    /**
     * @param srcPath
     * @param dstPath
     * @param fileType
     */
    public static void upload(String srcPath, String dstPath, String fileType) {
        //读取配置文件
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            //指定HDFS地址
            URI uri = new URI(hdfsURL);
            fs = FileSystem.get(uri, conf);
            // 获取本地文件系统
            local = FileSystem.getLocal(conf);
            // 文件类型正则表达式
            String regex = "^.*" + fileType + "$";
            //获取文件目录，正则匹配
            FileStatus[] listFile = local.globStatus(new Path(srcPath), new RegxAcceptPathFilter(regex));
            //获取文件路径
            Path[] listPath = FileUtil.stat2Paths(listFile);
            //HDFS 文件路径
            Path outPath = new Path(dstPath);
            if (fs.isDirectory(outPath)) {
                System.out.println(outPath + "目录已经存在");
            } else {
                makeDir(outPath);
            }
            //遍历所有文件路径
            for (Path p : listPath) {

                //上传至 HDFS
                fs.copyFromLocalFile(p, outPath);
                System.out.println("已上传：" + p.getName());
            }
            // fs.copyFromLocalFile(false,true,listPath,outPath);

        } catch (URISyntaxException ue) {
            ue.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        } finally {
            try {
                local.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * HDFS 目录创建
     *
     * @param outPath
     * @return
     * @throws IOException
     */
    public static Path makeDir(Path outPath) throws IOException {

        //目录权限:ower,group,other
        FsPermission filePermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        //判断目录是否存在，如果没有，则创建。
        boolean success = fs.mkdirs(outPath, filePermission);
        if (success) {
            System.out.println("创建 " + outPath + " 目录成功");
        } else {
            System.out.println("创建 " + outPath + " 目录失败");
        }
        return outPath;
    }
}
