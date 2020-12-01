package example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class JointTablesReducer extends Reducer<Text, Text, Text, Text> {
    static String price= new String();
    static String goodName=new String();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text item:values){
            String goodID = key.toString().split(",")[0];
            String orderID = key.toString().split(",")[1];
            if(orderID.equals("0")){
                goodName=item.toString().split(" ")[0];
                price=item.toString().split(" ")[1];
            }
            else {
                context.write(new Text(orderID),
                        new Text(item.toString().split(" ")[0]+" "+goodID+" "
                                +goodName+" "+price+" "+item.toString().split(" ")[1]));
            }
        }
    }

}
