package example;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JointTablesMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] item = value.toString().split(" ");
        if(item.length==3){
            Text proID = new Text(item[0]+","+"0");
            Text others = new Text(item[1]+" "+item[2]);
            context.write(proID,others);
        }
        else {
            Text proIDPlusOrderID = new Text(item[2]+","+item[0]);
            Text others = new Text(item[1]+" "+item[3]);
            context.write(proIDPlusOrderID,others);
        }

    }
}
