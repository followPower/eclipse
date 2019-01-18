package cn.itcast.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientDemo {

    FileSystem fs = null;
    
    @Before
    public void init() throws IOException, InterruptedException, URISyntaxException{
        
        Configuration conf = new Configuration();
       // conf.set("fs.defaultFS", "hdfs://centos1:9000");
      //  fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://centos1:9000"), conf, "hadoop");
        
    }
    
    @Test
    public void testupload() throws Exception{
        fs.copyFromLocalFile(new Path("C:/ImbaMallLog.txt"), new Path("/ImbaMallLog.txt.copy"));
        
        fs.close();
        
    }
    
    @Test
    public void testDownload () throws IllegalArgumentException, IOException{
        fs.copyToLocalFile(new Path("/ImbaMallLog.txt.copy"), new Path("D:/"));
    }
    
    
    
    
    
}
