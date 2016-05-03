package com.ai.baas.smc.calculate.topology.core.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;

public class DateUtil {

	
	public static Timestamp getFirstDay(String inDate,String inputFormat) throws ParseException{
		SimpleDateFormat df = new SimpleDateFormat(inputFormat);
		Date date = df.parse(inDate);
		Calendar calendar = Calendar.getInstance();
	    calendar.setTime(date);
	    calendar.add(Calendar.MONTH, 0);
	    calendar.set(Calendar.DAY_OF_MONTH,1);
	    //Date theDate = calendar.getTime();
	    return new Timestamp(calendar.getTimeInMillis());
	}
	
	public static Timestamp getLastDay(String inDate,String inputFormat) throws ParseException{
		SimpleDateFormat df = new SimpleDateFormat(inputFormat);
		Date date = df.parse(inDate);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
	    return new Timestamp(calendar.getTimeInMillis());
	}
	
	public static void main(String[] args) throws ParseException {
        //System.out.println(getLastDay("201604","yyyyMM","yyyyMMdd"));
		
		
	}

}
