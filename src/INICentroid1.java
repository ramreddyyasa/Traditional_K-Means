
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class INICentroid1{
	
	public static String num_centers="1";
	public static String cluster_id;
	private static String dfsLocation="hdfs://localhost:54310";
	
	
public static class MeanMap extends Mapper<LongWritable, Text, LongWritable,Text> {
		@Override
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(0), new Text(value));
		}
}
	public static class MeanReducer extends Reducer<LongWritable, Text,LongWritable, Text> {
		long size=0;
		String c_path;
			public void setup(Context context) throws IOException{
				
		 c_path=context.getConfiguration().get("pathc");
			
		}
		
	    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException,NumberFormatException {
			 Configuration configuration = new Configuration();
				configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
				configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
				FileSystem fs=FileSystem.get(configuration);
				ArrayList<Text>values1=new ArrayList<Text>();
				 for(Text obj :values){
					 size=size+1;
					
					 values1.add(obj);
					
				 }
			 if(size>2000000000){
				 size=2000000000;
			 }
			
			 int item = new Random().nextInt((int)size);
			 int i = 0;
			
			 String centers="";
			 for(Text obj :values1){
				
			     if (i == item){
			    	 
			    	  String[] line = obj.toString().split(",");
			    	 
	    	int length=line.length;
	    	for(int j=0;j<length;j++){
	    	
	    		if(j!=length-1)
	    		centers+=Integer.parseInt(line[j])+",";
	    		else
	    			centers+=Integer.parseInt(line[j])+"\t";
	    	}
			  break;    
			     }
			     i=i+1;
			 }
			 
			Path path1=new Path(c_path);
				if(!fs.exists(path1)){
					 fs.createNewFile(path1);
				}
				BufferedReader bfr=new BufferedReader(new InputStreamReader(fs.open(path1)));
	            String str = null;
	            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path1,true))); 
	            while ((str = bfr.readLine())!= null)
	            {
	                br.write(str);
	                br.newLine();
	              
	             }
	            bfr.close();
	            br.write("0\t"+centers);
	          
	            br.flush();
	            br.close();
	    	context.write(key,new Text(centers));
	    	
		}
	}
	public static void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		int i=1;
		int max=Integer.parseInt(args[1])-1;
		while(i<=max){
			
		Configuration conf_med=new Configuration();
		conf_med.setInt("c_id",i);
        conf_med.set("path",dfsLocation+args[2]+"_tmp_center-");
        conf_med.set("pathc",dfsLocation+args[2]+"_tmp_centroids/part-r-00000");
		Job job = Job.getInstance(conf_med,"Centroids"); 
        job.setJarByClass(INICentroid1.class);
        job.setMapperClass(NextCenterMapper.class); 
        job.setCombinerClass(NextCenterCombiner.class);
        job.setReducerClass(NextCenterReducer.class); 
        job.setMapOutputKeyClass(LongWritable.class);  
        job.setMapOutputValueClass(Text.class); 
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(dfsLocation+args[0])); //Input from DFS location 
		FileOutputFormat.setOutputPath(job, new Path(dfsLocation+args[2]+"_tmp_center-"+i));
		job.waitForCompletion(true);
		i++;
		
		}
	}
	public static void main(String[] args) throws Exception {
		
		long start = System.currentTimeMillis( ); 
		if(args.length<1 || args.length>3) //asking for hdfs port if the port is not 9000
		{
			System.out.println("usage: Mean <input> <output>");
			System.exit(0);
		}		
		Configuration conf = new Configuration();
		conf.set("pathc",dfsLocation+args[2]+"_tmp_centroids/part-r-00000");
		Job job = new Job(conf,"center1");
		job.setJobName("Mean");
        job.setJarByClass(INICentroid1.class);
        job.setMapperClass(MeanMap.class); 
        job.setReducerClass(MeanReducer.class); 
        job.setMapOutputKeyClass(LongWritable.class);  
        job.setMapOutputValueClass(Text.class); 
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(dfsLocation+args[0])); //Input from DFS location 
		FileOutputFormat.setOutputPath(job, new Path(dfsLocation+args[2]+"_tmp_center-0")); //Output to DFS location
		
		job.waitForCompletion(true);
		run(args);	
		 long end = System.currentTimeMillis( );
	        long diff = end - start;
	        System.out.println("Difference is : " + diff);
	}
}
	