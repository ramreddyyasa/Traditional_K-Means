import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class kmeans{
	private static int [][] _centroids;
	private static int itr=0;
	private static String CENTROID="";
	

	private static String dfsLocation="hdfs://localhost:54310";
	 private static double dist(int [] v1, int [] v2){
		    long sum=0;
		    for (int i=0; i<v2.length; i++){
		      int d = v1[i]-v2[i];
		      sum += d*d;
		    }
		    return Math.sqrt(sum);
		  }
		  
	private static boolean converge(int [][] c1,int [][] c2){
	
		 if(Arrays.deepEquals(c1,c2)){
		System.out.println("yes");
		   return true;
	   }
	   else{
				   return false;
	   }
	  }
	  


	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {


		
	    private int nums[];

		@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	Configuration conf = new Configuration();
	    	conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
	    	    	String path;
	    		path=CENTROID+"/part-r-00000";
	       	_centroids=readCentroids(path);
	    }
		
    @Override
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	int [][] centroids=_centroids;	
	        String[] line = value.toString().split(",");
	        nums = null;
	        int length=line.length;
	        nums=new int[length];
	        for(int i=0;i<length;i++)
	        	nums[i]=Integer.parseInt(line[i]);
	        int cluster_id = 0;
	        double minDistance = -1;
	            for (int i = 0; i < centroids.length; i++) {
	            double distance = dist(centroids[i],nums);
	            if (distance < minDistance || minDistance == -1) {
	                cluster_id = i;
	                minDistance = distance;
	            }
	        }
	    	        context.write(new IntWritable(cluster_id), value);
	    }
	}
	
	
	public static class KMeansReducer extends Reducer<IntWritable, Text,IntWritable, Text> {

	    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    
	    	
	    	long number_records=0;
	    	int length = 0;
			long sum[] = null;	
	    	for(Text value: values)
	    	{
	    	String[] line=value.toString().split(",");
	    	length=line.length;
	    	sum=new long[length];
	    	for(int i=0;i<length;i++){
	    		sum[i]+=Integer.parseInt(line[i]);
	    	}
	    	number_records++;
	    	break;
	       	   	}
	    	for(Text value: values)
	    	{
	    	String[] line=value.toString().split(",");
	    	for(int i=0;i<length;i++){
	    		sum[i]+=Integer.parseInt(line[i]);
	    	}
	    	number_records++;
	       	   	}
	    	String centroid="";
	       	for(int i=0;i<length;i++){
	    		sum[i]/=number_records;
	    		if(i!=length-1)
	    		centroid+=sum[i]+",";
	    		else
	    			centroid+=sum[i];
	    	}
	    	context.write(key,new Text(centroid+"\t|"+number_records));
		}
	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis( );
		if(args.length>3 || args.length<2) //asking for hdfs port if the port is not 9000
		{
			System.out.println("usage: Kmean <input> <output> <centroid-file> <port(optional)>");
			System.exit(0);
		}
		boolean isconverged=false;
		int[][] centroid;
		
		CENTROID=dfsLocation+args[1]+"_tmp_centroids";
		
		copyTemp(CENTROID);
		do{
			itr++;
		Configuration conf = new Configuration();	
		Job job = Job.getInstance(conf); //Creating the job
		job.setJobName("KMeans1");
        job.setJarByClass(kmeans.class); //Assining the jar class for main execution.
        job.setMapperClass(KMeansMapper.class); //Assining the Mapper class
        job.setReducerClass(KMeansReducer.class); //Assining the Reducer class
        job.setMapOutputKeyClass(IntWritable.class); //Defining the output key data type of mapper 
        job.setMapOutputValueClass(Text.class); //Defining the output value data type of mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(dfsLocation+args[0])); //Input from DFS location 
		FileOutputFormat.setOutputPath(job, new Path(dfsLocation+args[1])); //Output to DFS location
		
		
		job.waitForCompletion(true);
		centroid=readCentroids(dfsLocation+args[1]+"/part-r-00000"); // Reading the output of the job to centroid array
		if(converge(_centroids, centroid))  //Checking the converge condition
		{
			System.out.println("hai");
			removeTemp(CENTROID);	
			isconverged=true; 
		}
		else
		{
			
			copyCentroid(dfsLocation+args[1]);
			deleteFile(dfsLocation+args[1]);
					isconverged=false;
		}
		}while(!isconverged);
		 long end = System.currentTimeMillis( );
	        long diff = end - start;
	        System.out.println("Difference is : " + diff);
	        System.out.println("\nIterations:"+itr);
	}
	
	
	public static boolean deleteFile(String output) throws IOException
	{
		Configuration configuration = new Configuration();
		configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		FileSystem fs=FileSystem.get(configuration);
        

        try {
			if(fs.delete(new Path(output),true))
			{
				//fs.close();
				return true;
			}
			else{
				//fs.close();
				return false;
			}
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			//fs.close();
			return false;
		}
		
	}
	
	
	public static boolean copyCentroid(String output) throws IOException
	{
		Configuration configuration = new Configuration();
		configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		FileSystem fs=FileSystem.get(configuration);
        

        try {
			
			FileUtil.copy(fs, new Path(output+"/part-r-00000"), fs,new Path(CENTROID+"/part-r-00000"), true, configuration);
			//fs.close();
			return true;
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			//fs.close();
			return false;
		}
	}
	
	
	public static boolean removeTemp(String output) throws IOException
	{
		Configuration configuration = new Configuration();
		configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		FileSystem fs=FileSystem.get(configuration);
        

        try {
			FileUtil.copy(fs, new Path(CENTROID+"-temp"), fs, new Path(CENTROID), true, configuration);
			
			return true;
			
		} catch (Exception e1) {
			return false;
		}
	}
	
	public static boolean copyTemp(String output) throws IOException
	{
		Configuration configuration = new Configuration();
		configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		FileSystem fs=FileSystem.get(configuration);
        

        try {
        	FileUtil.copy(fs, new Path(CENTROID), fs, new Path(CENTROID+"-temp"),false, configuration);

			return true;
			
		} catch (Exception e1) {
			return false;
		}
	}
	
	public static int[][] readCentroids(String loc)throws IOException, InterruptedException
	{
		

    	String value;
        
        try{
        	Configuration configuration = new Configuration();
			configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
			configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
			Path path = new Path(loc);
			FileSystem fs=FileSystem.get(configuration);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            ArrayList<int[]> arr = new ArrayList<int[]>();
           
            while ((value = br.readLine()) != null) {	
            	String[] line = value.split("\t")[1].split(",");
            	int length=line.length;
            	int[] temp=new int[length];
            	System.out.println();
            	for(int i=0;i<length;i++)
            	temp[i] = Integer.parseInt(line[i]);
            	arr.add(temp);
    	       
            }
            int[][] centroid = new int[arr.size()][];
            for(int j=0;j<arr.size();j++)
            {
            	centroid[j]=arr.get(j);
            }
            return centroid;
            
         }catch(Exception e){
        	 System.out.println("error  "+e);
        	 return null;
         }
	}
}