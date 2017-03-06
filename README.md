# 批量上传文件到 HDFS

## 目的

- 批量上传 `/root` 下所有 `txt` 文件到 HDFS 的 `hadoopFile` 目录下。

## 准备

1\. 工程文件：

   - [MultipleFileUpload](src/main/java/com\/szkingdom/MultipleFileUpload.java):程序主入口。
   - [RegxAcceptPathFilter](src/main/java/com\/szkingdom/RegxAcceptPathFilter.java)：上传文件类型过滤。
2\. pom 文件

    - ![pom.xml](pom.xml)

3\. mvm 打包工程

```bash
mvn package 
```

   
## 运行

1\. 启动 Hadoop HDFS 

```bash
$ cd $HADOOP_HOME
$ sbin/start-dfs.sh
```

> hdfs 地址： hdfs://192.168.1.197:9000

2\. 运行程序

```bash
$ hadoop jar ~/hadoop-test-1.0-SNAPSHOT-jar-with-dependencies.jar 
```

3\. 查看

[http://192.168.1.197:50070](http://192.168.1.197:50070) --> Utilities --> Browse the file System