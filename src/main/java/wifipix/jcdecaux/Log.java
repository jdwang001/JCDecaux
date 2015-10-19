package wifipix.jcdecaux;

import org.apache.log4j.Logger;

/**
 * Created by aurora on 15/10/18.
 */
public class Log {
    private Logger logger;
    //封装log为单例模式
    private static Log log;

    private Log() {
        //String logfilePath = this.getClass().getResource("/").getPath();
        logger = Logger.getLogger(this.getClass());
    }

    public static Log getLoger() {
        if (log != null) {
            return log;
        } else {
            return new Log();
        }
    }

    //测试函数 直接运行即可，右键单击
    //log4j也可配置向指定账户发送邮件，将日志写入数据库，备份。。。。。
    public static void main(String args[]) {
        Log log = Log.getLoger();
        try {
            int a = 2 / 0;
        } catch (Exception e) {
            e.printStackTrace();
            log.logger.error("test log4j error ",e);
        }
    }

}
