package proai.test;

import org.junit.Assert;
import org.junit.Test;
import proai.service.DateRange;
import proai.service.DateRangeParseException;

import java.util.Date;

/**
 * @author Jan Schnasse schnasse@hbz-nrw.de
 */
@SuppressWarnings("javadoc")
public class TestDateRange {


    @Test
    public void testInclInclRange() {
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
    public void testExclInclRange() {
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

    @Test
    public void testNull1() {
        DateRange.getRangeExclIncl(null, "2013-06-13T09:05Z");
    }

    @Test
    public void testNull2() {
        DateRange.getRangeExclIncl("2013-06", null);
    }


    @Test(expected = DateRangeParseException.class)
    public void testNull3() {
        DateRange.getRangeExclIncl("2013/06/13",
                "2013-06-13T09:05Z");
    }


    @Test(expected = DateRangeParseException.class)
    public void testNull4() {
        DateRange.getRangeExclIncl("2013-06-13T01",
                "2013-06-13T09:05Z");
    }

    @Test
    public void testNull5() {
        Date date = null;
        DateRange range = DateRange.getRangeExclIncl(date, date);
        range.getFrom();
        range.getUntil();
    }

    @Test(expected = DateRangeParseException.class)
    public void testJunk1() {
        DateRange.getRangeExclIncl("",
                "junk");
    }

    @Test(expected = DateRangeParseException.class)
    public void testJunk2() {
        DateRange.getRangeExclIncl("junk",
                "");
    }

    @Test(expected = DateRangeParseException.class)
    public void testInvalid() {
        DateRange.getRangeExclIncl("2013-06-13T09:05:00.000Z",
                "2012-06-13T09:05:00.000Z");
    }

    @Test
    public void testDifferentGranularities() {
        DateRange.getRangeInclIncl("2002-02-05",
                "2007-09-22T19:04:34Z");
    }

}
