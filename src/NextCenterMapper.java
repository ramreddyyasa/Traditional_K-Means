
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NextCenterMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
	private static int cluster_id;
	int rnum[];
	 private static double dist(int [] v1, int [] v2){
		    double sum=0;
		    for (int i=0; i<v1.length; i++){
		      double d = v1[i]-v2[i];
		      sum += d*d;
		    }
		    return Math.sqrt(sum);
		  }
	

	@Override
    protected void setup(Context context) throws IOException, InterruptedException {
		cluster_id=Integer.parseInt(context.getConfiguration().get("c_id"));
		Configuration configuration = new Configuration();
		configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		String max_path=context.getConfiguration().get("path");
		
		Path path = new Path(max_path+(cluster_id-1)+"/part-r-00000");
		
		FileSystem fs=FileSystem.get(configuration);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
        String line[]=br.readLine().toString().split("\t");
        String m[]=line[1].split(",");
    	 
	    	int length=line.length;
	    	 rnum=new int[length];
	    	
	    	for(int j=0;j<length;j++){
	    		rnum[j]+=Integer.parseInt(m[j]);
    	}
    	
    	br.close();
    }
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        int length=line.length;
    	int num[]=new int[length];
    	  String s="";
    	for(int j=0;j<length;j++){
    		num[j]=Integer.parseInt(line[j]);
    		if(j!=length-1)
    			s+=num[j]+",";
    		else
    			s+=num[j];
    	}
            double distance = dist(rnum,num);
       
        context.write(new LongWritable(cluster_id),new Text(distance+","+s));
	} 
}