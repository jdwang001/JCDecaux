package wifipix.jcdecaux;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by aurora on 15/10/21.
 */
public class TestAP1AP2 {

    public static void main(String[] args) throws Exception {
        String line = "e4:95:6e:4f:53:0d 1442537292 1442537318 e4:95:6e:4f:54:95 1442537403 1442537403 e4:95:6e:4f:54:68 1442537341 1442537367 e4:95:6e:4f:54:67 1442537337 1442537337 e4:95:6e:4f:54:66 1442537340 1442537340 e4:95:6e:4f:54:65 1442537318 1442537318";//values.toString();
//        String line ="00:00:80:b0:ba:80       e4:95:6e:4f:53:0d 1442537292 1442537318e4:95:6e:4f:54:95 1442537403 1442537403e4:95:6e:4f:54:68 1442537341 1442537367e4:95:6e:4f:54:67 1442537337 1442537337e4:95:6e:4f:54:66 1442537340 1442537340e4:95:6e:4f:54:65 1442537318 1442537318";
        String specailAPMac = "e4:95:6e:4f:54:95";
        String[] allAPMac = {"e4:95:6e:4f:54:95", "e4:95:6e:4f:53:0d", "e4:95:6e:4f:54:68", "e4:95:6e:4f:54:67", "e4:95:6e:4f:54:66", "e4:95:6e:4f:54:65"};
        String data1frame = null;
        String[] tmp;
        String [] field = line.split(" ");

        Map<String, Long> stime = new HashMap<String, Long>();
        Map<String, Long> etime = new HashMap<String, Long>();
        int sum = 0;
//        String test;

//         test = line.replaceAll("^[^\\s]+\\s+(.*)","$1");
//        System.out.println("the line is " + test);

//        for (int i = 0; i < tmp.length; i++) {
//            [i+1] += " "
//        }

        System.out.println("field length is " + field.length + " field[0] is " + field[0]);




        if (field.length > 3) {
            for (int i = 0; i < field.length/3 ; i ++) {
                stime.put(field[i * 3], Long.parseLong(field[i * 3 + 1]));
                etime.put(field[i * 3], Long.parseLong(field[i * 3 + 2]));
            }

            System.out.println("全部AP个数 " + allAPMac.length);

            for (Object key : stime.keySet()) {
                System.out.println("The apMac key is " + key + "value is " + stime.get(key));
                    // 探针格式
                    for (int i = 0; i < allAPMac.length; i++) {
                        if (stime.get(allAPMac[i]) != null) {
                            //不为空，且不为自身
                            if (!key.equals(allAPMac[i])) {
                                //如果起始时间，小于任一探针，则为经过A
                                if (stime.get(key) < stime.get(allAPMac[i])) {
                                    sum++;
                                    System.out.println("-------------------");
                                    System.out.println("先经过AP " + key + " 客流总数 " + sum);
                                    System.out.println("-------------------");
                                    sum = 0;
                                    break;
                                }

                                if (etime.get(key) > etime.get(allAPMac[i])) {
                                    sum++;
                                    System.out.println("-------------------");
                                    System.out.println("后经过AP " + key + " 客流总数 " + sum);
                                    System.out.println("-------------------");
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
