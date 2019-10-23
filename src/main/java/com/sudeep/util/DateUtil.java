package com.sudeep.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtil {
    public final static String TOMORROW_OPEN = "TM_OP";
    public final static String TOMORROW_CLOSE = "TM_CLOSE";

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static long getMillisInterval(Calendar startTime, Calendar endTime){
        return endTime.getTimeInMillis() - startTime.getTimeInMillis();
    }

    public static int getMinuteInterval(Calendar startTime, Calendar endTime){
        return (int) (getMillisInterval(startTime, endTime) / 60000);
    }

    public static String calendarToString(Calendar calendar, SimpleDateFormat format){
        return format.format(calendar.getTime());
    }

    public static Calendar stringToCalendar(String str, SimpleDateFormat format) throws ParseException{
        if (str.equals(TOMORROW_OPEN))
            return getTomorrowOpenTime();
        if (str.equals(TOMORROW_CLOSE))
            return getTomorrowCloseTime();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(format.parse(str));
        return calendar;
    }

    public static Calendar getTomorrowOpenTime(){
        Calendar tm = getTomorrow();
        tm.set(Calendar.HOUR_OF_DAY, 9);
        tm.set(Calendar.MINUTE, 30);
        return tm;
    }

    public static Calendar getTomorrowCloseTime(){
        Calendar tm = getTomorrow();
        tm.set(Calendar.HOUR_OF_DAY, 15);
        tm.set(Calendar.MINUTE, 30);
        return tm;
    }

    public static Calendar getTomorrow(){
        Calendar calendar = Calendar.getInstance();

        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        return calendar;
    }

    public static Calendar getSimulatedCurrentTimeForTomorrow(){
        Calendar calendar = Calendar.getInstance();

        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        return calendar;
    }
}
