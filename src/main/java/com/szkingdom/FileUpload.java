package com.szkingdom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @DESCRIPTION ${DESCRIPTION}
 * @Author TangPeng
 * @Date 2017-3-7
 */
public class FileUpload {

    private String srcPath;
    private String hdfsURL;
    private String dstPath;
    private String fileType;

    public FileUpload(String srcPath, String hdfsURL, String dstPath, String fileType) {
        this.srcPath = srcPath;
        this.hdfsURL = hdfsURL;
        this.dstPath = dstPath;
        this.fileType = fileType;
    }

    public void upload() {
//读取配置文件
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        try {
            //指定HDFS地址
            URI uri = new URI(hdfsURL);
            final FileSystem fs = FileSystem.get(uri, conf, "hadoop");
            // 获取本地文件系统
            FileSystem local = FileSystem.getLocal(conf);
            // 文件类型正则表达式
            String regex = "^.*" + fileType + "$";
            //获取文件目录，正则匹配
            FileStatus[] listFile = local.globStatus(new Path(srcPath), new RegxAcceptPathFilter(regex));
            //获取文件路径
            Path[] listPath = FileUtil.stat2Paths(listFile);
            //HDFS 文件路径
            final Path outPath = new Path(dstPath);
            if (fs.isDirectory(outPath)) {
                System.out.println(outPath + "目录已经存在");
            } else {
                makeDir(fs, outPath);
            }
            ExecutorService threadPool = Executors.newCachedThreadPool();
            //遍历所有文件路径
            for (final Path p : listPath) {
                System.out.println("正在上传：" + p.getName());
                //上传至 HDFS
//                fs.copyFromLocalFile(p, outPath);
                threadPool.execute(new UploadThread(fs, p, outPath));
               // new Thread(new UploadThread(fs, p, outPath)).start();

            }
            threadPool.shutdown();
        } catch (URISyntaxException ue) {
            ue.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        } catch (InterruptedException te) {
            te.printStackTrace();
        }/*finally {

            try {
                //local.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }*/

    }

    public Path makeDir(FileSystem fs, Path outPath) throws IOException {

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
