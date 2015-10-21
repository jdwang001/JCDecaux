package wifipix.jcdecaux.analyse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by aurora on 15/10/21.
 */
public class Number2Ap {
    public static class number2ApMapper extends Mapper<Object, Text, Text, LongWritable> {
        private Text apMac = new Text();
        //        private Text numCount = new Text();
        private LongWritable numCount = new LongWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().replaceAll("^[^\\s]+\\s+(.*)","$1");
//            String line = value.toString();

            String specailAPMac = "e4:95:6e:4f:54:95";
            String data1frame = null;
            String[] allAPMac = {"e4:95:6e:4f:54:97", "e4:95:6e:4f:52:ed",
                    "e4:95:6e:4f:52:f0", "e4:95:6e:4f:53:0d",
                    "e4:95:6e:4f:53:ef","e4:95:6e:4f:54:95","e4:95:6e:4f:54:68","e4:95:6e:4f:52:61","e4:95:6e:4f:54:4d","e4:95:6e:4f:54:65","e4:95:6e:4f:52:62","e4:95:6e:4f:54:67","e4:95:6e:4f:54:85","e4:95:6e:4f:54:66","e4:95:6e:4f:54:88","e4:95:6e:4f:54:8d","e4:95:6e:4f:53:7e"};
            String [] field = line.split(" ");

            int sum = 0;
            Map<String, Long> stime = new HashMap<String, Long>();
            Map<String, Long> etime = new HashMap<String, Long>();



            if (field.length > 3) {
                for (int i = 0; i < field.length/3 ; i ++) {
                    stime.put(field[i * 3], Long.parseLong(field[i * 3 + 1]));
                    etime.put(field[i * 3], Long.parseLong(field[i * 3 + 2]));
                }

//                System.out.println("全部AP个数 " + allAPMac.length);

                for (Object key4data : stime.keySet()) {
                    System.out.println("The apMac key4data is " + key4data + "value is " + stime.get(key4data));
                    // 探针格式
                    for (int i = 0; i < allAPMac.length; i++) {
                        if (stime.get(allAPMac[i]) != null) {
                            //不为空，且不为自身
                            if (!key4data.equals(allAPMac[i])) {
                                //如果起始时间，小于任一探针，则为经过A
                                if (stime.get(key4data) < stime.get(allAPMac[i])) {
                                    sum++;
                                    apMac.set(allAPMac[i]+"_st");
                                    numCount.set(sum );
                                    context.write(apMac, numCount);
                                    sum = 0;
                                    break;
                                }

                                if (etime.get(key4data) > etime.get(allAPMac[i])) {
                                    sum++;
                                    apMac.set(allAPMac[i]+"_et");
                                    numCount.set(sum );
                                    context.write(apMac, numCount);
                                    sum = 0;
                                    break;
                                }
                            }
                        }
                    }
                }

            }

        }
    }




}
