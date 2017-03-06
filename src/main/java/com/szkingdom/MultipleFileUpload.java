package com.szkingdom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.net.URI;

/**
 * @DESCRIPTION 批量上传文件至 HDFS
 * @Author TangPeng
 * @Date 2017-3-6
 */
public class MultipleFileUpload {
    //声明两个从不同文件系统类型的静态变量
    private static FileSystem fs = null;
    private static FileSystem local = null;
    private static String hdfsURL = "hdfs://192.168.1.197:9000/";

    private static Logger logger = Logger.getLogger(MultipleFileUpload.class);

    /*
    * 格式：hadoop jar ~/hadoop-test-1.0-SNAPSHOT-jar-with-dependencies.jar /home/server1 hadoopFile tgz
    * args[0]: 待上传文件目录，必填
    * args[1]: HDFS 目标文件目录，必填
    * args[2]: 待上传文件类型，必填
    * args[4]: hdfs 地址，可填
     */
    public static void main(String[] args) throws Exception {

        int len = args.length;

        if (args.length < 1) {
            logger.error("没有输入任何参数");
            System.exit(0);
        } else if (len > 4) {
            logger.error("输入过多无效参数");
            System.exit(0);
        } else {
            //指定在元数据目录的地址在linux环境下
            String srcPath = args[0] + "/*";
            String dstPath = hdfsURL + args[1] + "/";
            String fileType = args[2];
            if (len == 4 && !(args[3] == null || args[3].equals(""))) {
                hdfsURL = args[3];
            }
            logger.info("待上传文件目录：" + srcPath);
            logger.info("HDFS 目标文件目录" + dstPath);
            //上传到 HDFS
            upload(srcPath, dstPath, fileType);
        }
    }

    public static void upload(String srcPath, String dstPath, String fileType) throws Exception {
        //读取配置文件
        Configuration conf = new Configuration();
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
        //输出文件路径
        Path outPath = new Path(dstPath);
        //判断目录是否存在，如果没有，则创建。
        if (fs.exists(outPath)) {
            logger.info(outPath + "目录已经存在 ");
        } else {
            logger.info("正在创建目录：" + outPath);

            //FsPermission filePermission = null;
            //filePermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ);
            //创建目录 不设置权限,默认为当前hdfs服务器启动用户
            boolean success = fs.mkdirs(outPath, null);
            // 设置目录权限
            // boolean success = fs.mkdirs(outPath, filePermission);
            logger.info("创建" + outPath + " 目录成功：" + success);
        }

        //遍历所有文件路径
        for (Path p : listPath) {
            logger.info("正在上传：" + p.getName());
            //上传至 HDFS
            fs.copyFromLocalFile(p, outPath);
        }
    }
}
