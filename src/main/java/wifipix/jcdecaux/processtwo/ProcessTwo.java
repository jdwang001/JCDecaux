package wifipix.jcdecaux.processtwo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by aurora on 15/10/19.
 */
public class ProcessTwo {
    public static class getSimple1Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text getdata = new Text();
        private IntWritable one = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] field = line.split(" ");

            //8:31:02~8:35:56；
            String theSpecialAP = "e4:95:6e:4f:54:95";
            String timeStart800 = "2015-09-18 08:31:00";
            String timeend935 = "2015-09-18 08:35:00";

            if (iswantTime(timeStart800, timeend935, field[0])) {
                if (field[1].equals(theSpecialAP)) {
                    getdata.set(field[2]);
                    context.write(getdata, one);
                }
            }

        }

        public static boolean iswantTime(String star,String end, String datatime){
            SimpleDateFormat localTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try{
                Date sdate = localTime.parse(star);
                Date edate=localTime.parse(end);
                Date date = localTime.parse(toLocalTime(datatime));
                System.out.println(sdate.getTime()+"##"+date.getTime()+"##"+edate.getTime());
                if (date.after(sdate) && date.before(edate)) {
//                    System.out.println("true");
                    return true;
                }
            }catch(Exception e){}

            return false;
        }
        //转换为本地时间
        public static String toLocalTime(String unix) {
            Long timestamp = Long.parseLong(unix) * 1000;
            String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(timestamp));
            return date;
        }


        public static String toUnixTime(String local){
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String unix = "";
            try {
                unix = df.parse(local).getTime() + "";
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return unix;
        }
    }


    public static class getSimple1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable tmp : values) {
                sum += tmp.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
