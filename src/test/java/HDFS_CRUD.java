import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.*;
public class HDFS_CRUD{
	FileSystem fs = null;

	/**
	 *@Author zcq
	*@Description //TOdO 客户端连接
	        *@Param 
	        *@Return 
	        *@Author zcq
	        *@Date
	        */
	
	@Before
	public void init() throws Exception{

		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", "hdfs://master:9000");

		System.setProperty("HADOOP_USER_NAME", "root");

		fs = FileSystem.get(conf);
	}
	/*
	 *文件上传
	 */
	@Test
	public void testAddFileToHdfs() throws IOException{

		Path src = new Path("D:/test.txt");

		Path dst = new Path("/testFile");

		fs.copyFromLocalFile(src, dst);

		fs.close();
	}
	/*
	 *文件下载 
	 */
	@Test
	public void testDownloadFileToLocal() throws IOException{

		fs.copyToLocalFile(new Path("/testFile"), new Path("D:/"));
		fs.close();
	}
	/*
	 *文件目录
	 */
	@Test
	public void testMkdirAndDeleteAndRename() throws IOException{

		fs.mkdirs(new Path("/a/b/c"));
		fs.mkdirs(new Path("/a2/b2/c2"));

		fs.rename(new Path("/a"), new Path("/a3"));

		fs.delete(new Path("/a2"), true);
	}
	/*
	 *文件展示
	 */
	@Test
	public void testListFiles() throws
			IllegalArgumentException, IOException{

		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();

			System.out.println("文件名:"+fileStatus.getPath().getName());

			System.out.println("文件块大小:"+fileStatus.getBlockSize());

			System.out.println("文件权限:"+fileStatus.getPermission());

			System.out.println("文件内容长度:"+fileStatus.getLen());

			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for(BlockLocation bl : blockLocations) {
				System.out.println("block-length:"+bl.getLength()+"--"+"block-offset:"+bl.getOffset());
				String[] hosts = bl.getHosts();
				for(String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("-----------结束了方法----------");
			System.out.println("-------------------------------");
			System.out.println("hello git444");
			System.out.println("master test");
			System.out.println("hot-fix test");
		}
	}
	@Test
	public void test() {
		System.out.println("hello git");
	}
}