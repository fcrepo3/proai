package proai.test;

import java.text.ParseException;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;
import proai.service.DateRange;
import proai.service.DateRangeParseException;

/**
 * @author Jan Schnasse schnasse@hbz-nrw.de
 *
 */
public class TestDateRange {

    @Test
    public void testInclInclRange() throws ParseException {
	DateRange range = null;

	range = DateRange.getRangeInclIncl("2013", "2013-06-13T09:05Z");
	Assert.assertEquals("2012-12-31T23:59:59.999Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06", "2013-06-13T09:05Z");
	Assert.assertEquals("2013-05-31T23:59:59.999Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13", "2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-12T23:59:59.999Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T01Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T00:59:59.999Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T01:01Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T01:00:59.999Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T01:01:01Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T01:01:00.999Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T01:01:01.000Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T01:01:00.999Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T09:05Z", "2014");
	Assert.assertEquals("2013-06-13T09:04:59.999Z", range.getFrom());
	Assert.assertEquals("2015-01-01T00:00:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T09:05Z", "2014-01");
	Assert.assertEquals("2013-06-13T09:04:59.999Z", range.getFrom());
	Assert.assertEquals("2014-02-01T00:00:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T09:05Z", "2014-01-03");
	Assert.assertEquals("2013-06-13T09:04:59.999Z", range.getFrom());
	Assert.assertEquals("2014-01-04T00:00:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T09:05Z",
		"2014-01-03T13Z");
	Assert.assertEquals("2013-06-13T09:04:59.999Z", range.getFrom());
	Assert.assertEquals("2014-01-03T14:00:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T09:05Z",
		"2014-01-03T13:45Z");
	Assert.assertEquals("2013-06-13T09:04:59.999Z", range.getFrom());
	Assert.assertEquals("2014-01-03T13:46:00.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T09:05Z",
		"2014-01-03T13:45:34Z");
	Assert.assertEquals("2013-06-13T09:04:59.999Z", range.getFrom());
	Assert.assertEquals("2014-01-03T13:45:35.000Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013-06-13T09:05Z",
		"2014-01-03T13:45:34.023Z");
	Assert.assertEquals("2013-06-13T09:04:59.999Z", range.getFrom());
	Assert.assertEquals("2014-01-03T13:45:34.024Z", range.getUntil());

	range = DateRange.getRangeInclIncl("2013", "2013");
	Assert.assertEquals("2012-12-31T23:59:59.999Z", range.getFrom());
	Assert.assertEquals("2014-01-01T00:00:00.000Z", range.getUntil());
    }

    @Test
    public void testExclInclRange() throws ParseException {
	DateRange range = null;

	range = DateRange.getRangeExclIncl("2013", "2013-06-13T09:05Z");
	Assert.assertEquals("2013-01-01T00:00:00.000Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06", "2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-01T00:00:00.000Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13", "2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T00:00:00.000Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T01Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T01:00:00.000Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T01:01Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T01:01:00.000Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T01:01:01Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T01:01:01.000Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T01:01:01.000Z",
		"2013-06-13T09:05Z");
	Assert.assertEquals("2013-06-13T01:01:01.000Z", range.getFrom());
	Assert.assertEquals("2013-06-13T09:06:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T09:05Z", "2014");
	Assert.assertEquals("2013-06-13T09:05:00.000Z", range.getFrom());
	Assert.assertEquals("2015-01-01T00:00:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T09:05Z", "2014-01");
	Assert.assertEquals("2013-06-13T09:05:00.000Z", range.getFrom());
	Assert.assertEquals("2014-02-01T00:00:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T09:05Z", "2014-01-03");
	Assert.assertEquals("2013-06-13T09:05:00.000Z", range.getFrom());
	Assert.assertEquals("2014-01-04T00:00:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T09:05Z",
		"2014-01-03T13Z");
	Assert.assertEquals("2013-06-13T09:05:00.000Z", range.getFrom());
	Assert.assertEquals("2014-01-03T14:00:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T09:05Z",
		"2014-01-03T13:45Z");
	Assert.assertEquals("2013-06-13T09:05:00.000Z", range.getFrom());
	Assert.assertEquals("2014-01-03T13:46:00.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T09:05Z",
		"2014-01-03T13:45:34Z");
	Assert.assertEquals("2013-06-13T09:05:00.000Z", range.getFrom());
	Assert.assertEquals("2014-01-03T13:45:35.000Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013-06-13T09:05Z",
		"2014-01-03T13:45:34.023Z");
	Assert.assertEquals("2013-06-13T09:05:00.000Z", range.getFrom());
	Assert.assertEquals("2014-01-03T13:45:34.024Z", range.getUntil());

	range = DateRange.getRangeExclIncl("2013", "2013");
	Assert.assertEquals("2013-01-01T00:00:00.000Z", range.getFrom());
	Assert.assertEquals("2014-01-01T00:00:00.000Z", range.getUntil());
    }

    @Test(expected = NullPointerException.class)
    public void testNull1() throws ParseException {
	DateRange range = DateRange.getRangeExclIncl(null, "2013-06-13T09:05Z");
    }

    @Test(expected = NullPointerException.class)
    public void testNull2() throws ParseException {
	DateRange range = DateRange.getRangeExclIncl("2013-06", null);
    }

<<<<<<< HEAD
=======

>>>>>>> 016f119... exception handling
    @Test(expected = DateRangeParseException.class)
    public void testNull3() throws ParseException {
	DateRange range = DateRange.getRangeExclIncl("2013/06/13",
		"2013-06-13T09:05Z");
    }

<<<<<<< HEAD
=======

>>>>>>> 016f119... exception handling
    @Test(expected = DateRangeParseException.class)
    public void testNull4() throws ParseException {
	DateRange nrange = DateRange.getRangeExclIncl("2013-06-13T01",
		"2013-06-13T09:05Z");
    }

    @Test(expected = NullPointerException.class)
    public void testNull5() throws ParseException {
	Date date = null;
	DateRange range = DateRange.getRangeExclIncl(date, date);
	String str = range.getFrom();
	str = range.getUntil();
    }
}
