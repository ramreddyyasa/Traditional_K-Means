
import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NextCenterCombiner  extends Reducer<LongWritable,Text,LongWritable,Text>{
	@Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException,NumberFormatException {
	double maxDistance=-1;
	double distance;
	String result="";
	String s="";
	for(Text val:values){
		String line[]=val.toString().split(",");
		distance=Double.parseDouble(line[0]);
		int length=line.length;
		
		if(maxDistance<distance){
			maxDistance=distance;
			for(int i=1;i<length;i++){
				if(i!=length-1)
				s+=Integer.parseInt(line[i])+",";
				else
					s+=Integer.parseInt(line[i]);
			}
			result=distance+","+s;
			s="";
		}
	}
	context.write(key, new Text(result));
	}
}
