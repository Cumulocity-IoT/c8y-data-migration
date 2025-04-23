package com.softwareag.signalmigration.util;

import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class DateUtil {
	public static final DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
	public static final DateTimeFormatter format = ISODateTimeFormat.dateTime();
	public static final DateTimeFormatter formatUTC = ISODateTimeFormat.dateTime().withZoneUTC();
	
	
	public static final Date ISODateTimeStringToDate (String isoDate) {
		return parser.parseDateTime(isoDate).toDate();
	}	
	
	public static final String toISODateTimeString (DateTime date) {
		return format.print(date);	
	}
	
	public static final String toISODateTimeString (Date date) {
		return toISODateTimeString(new DateTime(date));
	}
	
	
	public static final String toISODateTimeString (String date) {
		return formatUTC.print(parser.parseDateTime(date));
	}
	
}
