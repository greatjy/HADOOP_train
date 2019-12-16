package com.alibaba.hadoopdemo.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: wjy
 * @Date: 2019/12/13 22:18
 * 使用HDFSAPI操作HDFS文件系统
 */
public class HDFSApp {
    public static final String hadoopURIAddress = "hdfs://192.168.18.136:8020";
    FileSystem fileSystem;
    Configuration configuration;

    /**
     * 构造一个访问hdfs文件系统客户端的对象
     * 参数1：访问hdfs文件系统逻辑地址的uri，定义再hdfs中的core-site.xml
     * 参数2：hdfs文件系统的配置文件
     * 参数3：执行hdfs文件系统的用户
     */
    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("--------setup--------");
        URI hadoopURI = new URI(hadoopURIAddress);
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(hadoopURI, configuration, "root");
    }

    @After
    public void tearDown() {
        fileSystem = null;
        configuration = null;
        System.out.println("----------Tear Down --------------");
    }

    /**
     * 创建hdfs文件系统文件夹
     *
     * @throws IOException
     */
    @Test
    public void mkDir() throws IOException {
        fileSystem.mkdirs(new Path("/testHadoopApi"));
    }

    /**
     * 查看hdfs文件夹内的内容
     *
     * @throws IOException
     */
    @Test
    public void ls() throws IOException {
        FileStatus[] status = fileSystem.listStatus(new Path("/"));
        for (FileStatus fs : status) {
            String isDir = fs.isDirectory() ? "dir" : "file";
            String permission = fs.getPermission().toString();
            short replication = fs.getReplication();
            long length = fs.getLen();
            String path = fs.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t" + path);
        }
    }

    /**
     * 查看hdfs文件系统中的文件内容，并打印在控制台上
     *
     * @throws IOException
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream openFile = fileSystem.open(new Path("/test_hadoop/hello_world.cpp"));
        IOUtils.copyBytes(openFile, System.out, 1024);
    }

    /**
     * 新建hdfs文件夹文件，并通过FSDataOutputStream写入的方式，向新建文件中写入内容
     *
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream outFile = fileSystem.create(new Path("/test_hadoop/hello_world_2.txt"));
        outFile.writeChars("hello_world_2/n");
        outFile.flush();
        outFile.close();
    }

    /**
     * 更改hdfs文件系统中的文件名
     *
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        Path path = new Path("/test_hadoop/hello.txt");
        Path newPath = new Path("/test_hadoop/hello_hdfs.txt");
        boolean result = fileSystem.rename(path, newPath);
        System.out.println(result);
    }

    /**
     * 拷贝本地文件到HDFS
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path src = new Path("D:\\music\\tianhou天后.mp3");
        Path dest = new Path("/test_hadoop/天后.mp3");
        fileSystem.copyFromLocalFile(src, dest);
    }


    /**
     * 将大文件上传到hdfs系统上，需要显示进度。通过create一个输出流，然后通过将本地文件地输入流写入地方式
     * 将本地文件上传到hdfs文件系统上。create方法中接收一个Progressable实现类对象，用来定义显示进度地形式
     * 报告进展的设施。
     *
     * @throws IOException
     */
    @Test
    public void copyBigFileFromLocalFile() throws IOException {

        File jdkFile = new File("D:\\360安全浏览器下载\\jdk-8u231-linux-x64.tar.gz");
        InputStream inputStream = new BufferedInputStream(new FileInputStream(jdkFile));
        Path hadoopPath = new Path("/jdk.gz");
        FSDataOutputStream outputStream = fileSystem.create(hadoopPath, new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(inputStream, outputStream, 4096);
    }

    /**
     * 将HDFS文件系统上的文件拷贝到本地文件系统
     *
     * @throws IOException
     */
    @Test
    public void copyToLocalFile() throws IOException {
        Path hdfsPath = new Path("/test_hadoop/hello_world.txt");
        Path localPath = new Path("C:\\Users\\MrWang\\Desktop\\hello_world.txt");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }

    /**
     * 递归展示文件夹下面的所有文件。
     *
     * @throws IOException
     */
    @Test
    public void recusiveListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);
        while(files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println(file.toString());
        }
    }


    /**
     * 得到文件再hdfs文件系统上的块的分布
     * @throws IOException
     */
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus filestatus = fileSystem.getFileStatus(new Path("/jdk.gz"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(filestatus, 0, filestatus.getLen());
        for(BlockLocation block : blocks) {
            for(String name : block.getNames()){
                System.out.println(name+" "+block.getOffset()+" "+block.getLength());
            }
        }
    }


    /**
     * 删除文件
     * @throws IOException
     */
    @Test
    public void deleteFiles() throws IOException {
        fileSystem.delete(new Path("/jdk.gz"),false);
    }


}
