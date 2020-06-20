import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

public class DateFormat {


    public static void main(String[] args) {
        System.out.println(DateFormatUtils.format(new Date(), DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern()));
    }

}
