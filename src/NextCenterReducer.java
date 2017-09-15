
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NextCenterReducer extends Reducer<LongWritable,Text,LongWritable,Text>{
	String c_path;
	
	public void setup(Context context) throws IOException{
		 c_path=context.getConfiguration().get("pathc");
}
	 protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException,NumberFormatException {
		 		
		 	String s="";
			double maxDistance=-1;
			double distance;
		
			
			for(Text val:values){
				String line[]=val.toString().split(",");
				int length=line.length;
				//System.out.println("length:"+length);
				//System.out.println("line:"+line[0]+"\t"+line[1]+"\t"+line[2]+"\t"+line[3]+"\t\n");
				distance=Double.parseDouble(line[0]);
				String result="";
				if(maxDistance<distance){
					maxDistance=distance;
					for(int i=1;i<length;i++){
						if(i!=length-1)
							result+=Integer.parseInt(line[i])+",";
						else
							result+=Integer.parseInt(line[i]);
					
					}
					s=result;
					
				}
			}
	
			Configuration configuration = new Configuration();
			configuration.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
			configuration.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
			FileSystem fs=FileSystem.get(configuration);
	
			Path path=new Path(c_path);
			BufferedReader bfr=new BufferedReader(new InputStreamReader(fs.open(path)));
            String str = null;
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true))); 
            while ((str = bfr.readLine())!= null)
            {
                br.write(str);
                br.newLine();
               
             }
            bfr.close();
            br.write(key+"\t"+s);
            br.flush();
            br.close();
		context.write(key, new Text(s));
}
}