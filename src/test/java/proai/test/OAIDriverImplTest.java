package proai.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import proai.MetadataFormat;
import proai.Record;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.impl.OAIDriverImpl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.*;

public class OAIDriverImplTest {

    public static final String LATEST_DATE = "2005-03-20T21:13:59Z";

    public static final String OAI_DC_PREFIX = "oai_dc";
    public static final String OAI_DC_URI = "http://www.openarchives.org/OAI/2.0/oai_dc/";
    public static final String OAI_DC_LOC = "http://www.openarchives.org/OAI/2.0/oai_dc.xsd";
    public static final String TEST_FORMAT_PREFIX = "test_format";
    public static final String TEST_FORMAT_URI = "http://example.org/testFormat/";
    public static final String TEST_FORMAT_LOC = "http://example.org/testFormat.xsd";

    public static final String ABOVE_TWO_SPEC = "abovetwo";
    public static final String ABOVE_TWO_EVEN_SPEC = "abovetwo:even";
    public static final String ABOVE_TWO_ODD_SPEC = "abovetwo:odd";
    public static final String PRIME_SPEC = "prime";
    public static final String NOPARENT_SPEC = "noancestor:noparent:test";

    private OAIDriver m_impl;

    @Before
    public void setUp() throws IOException {
        m_impl = new OAIDriverImpl();
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/proai.properties"));

        File baseDir = new File(getClass().getResource("/").getFile());

        properties.setProperty("proai.driver.simple.baseDir", baseDir.getAbsolutePath());

        m_impl.init(properties);
    }

    @After
    public void tearDown() {
        m_impl.close();
    }

    @Test
    public void testLatestDate() throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String latestDate = df.format(m_impl.getLatestDate());
        System.out.println("Latest Date was " + latestDate);
        assertEquals(LATEST_DATE, latestDate);
    }

    @Test
    public void testIdentity() throws Exception {
        // TODO Test has no assertion
        StringWriter writer = new StringWriter();
        m_impl.write(new PrintWriter(writer, true));
        System.out.println("Result of writeIdentity:\n" + writer.toString());
    }

    @Test
    public void testFormats() throws Exception {
        Iterator<? extends MetadataFormat> iter = m_impl.listMetadataFormats();
        while (iter.hasNext()) {
            MetadataFormat format = iter.next();
            String prefix = format.getPrefix();
            String uri = format.getNamespaceURI();
            String loc = format.getSchemaLocation();
            System.out.println("Format prefix = " + prefix);
            System.out.println("       uri    = " + uri);
            System.out.println("       loc    = " + loc);
            assertTrue(prefix.equals(OAI_DC_PREFIX)
                    || prefix.equals(TEST_FORMAT_PREFIX));
            if (prefix.equals(OAI_DC_PREFIX)) {
                assertEquals(uri, OAI_DC_URI);
                assertEquals(loc, OAI_DC_LOC);
            } else {
                assertEquals(uri, TEST_FORMAT_URI);
                assertEquals(loc, TEST_FORMAT_LOC);
            }
        }
    }

    @Test
    public void testSets() throws Exception {
        Iterator<? extends SetInfo> iter = m_impl.listSetInfo();
        while (iter.hasNext()) {
            SetInfo info = iter.next();
            String spec = info.getSetSpec();
            System.out.println("Set spec = " + spec);

            assertInSet(spec, new String[]{
                    ABOVE_TWO_SPEC,
                    ABOVE_TWO_EVEN_SPEC,
                    ABOVE_TWO_ODD_SPEC,
                    PRIME_SPEC,
                    NOPARENT_SPEC});

            StringWriter writer = new StringWriter();
            info.write(new PrintWriter(writer, true));
            System.out.println("Result of write: \n" + writer.toString());
        }
    }

    private void assertInSet(String spec, String[] strings) {
        boolean contains = false;
        for (String s : strings) {
            if (spec.equals(s)) {
                contains = true;
                break;
            }
        }
        if (!contains) fail("String " + spec + "is not in the set");
    }

    @Test
    public void listingZeroUnknownFormatRecords() throws Exception {
        Iterator<? extends Record> iter;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date fromDate = df.parse("2005-01-01T08:52:20Z");
        Date untilDate = new Date();
        untilDate.setTime(fromDate.getTime() + 1000);
        System.out.println("Listing zero unknown_format records");
        iter = m_impl.listRecords(fromDate, untilDate, "unknown_format");
        checkRecords(iter, new int[]{});
    }

    private void checkRecords(Iterator<? extends Record> iter, int[] expecting) throws Exception {
        boolean[] saw = new boolean[expecting.length];
        while (iter.hasNext()) {
            Record rec = iter.next();
            String id = rec.getItemID();
            System.out.println("  Found Record with itemID = " + id);
            int n = Integer.parseInt(id.substring(id.length() - 1));
            boolean wasExpected = false;
            for (int i = 0; i < expecting.length; i++) {
                if (n == expecting[i]) {
                    saw[i] = true;
                    wasExpected = true;
                }
            }
            assertTrue(wasExpected);
        }
        for (int i = 0; i < saw.length; i++) {
            assertTrue(saw[i]);
        }
    }

    @Test
    public void listingOneTestFormatRecord() throws Exception {
        Iterator<? extends Record> iter;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date fromDate = df.parse("2005-01-01T08:52:20Z");
        Date untilDate = new Date();
        untilDate.setTime(fromDate.getTime() + 1000);
        System.out.println("Listing one test_format record");
        iter = m_impl.listRecords(fromDate, untilDate, TEST_FORMAT_PREFIX);
        checkRecords(iter, new int[]{2});
    }

    @Test
    public void listingOneOaiDcRecord() throws Exception {
        Iterator<? extends Record> iter;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date fromDate = df.parse("2005-01-01T08:52:20Z");
        Date untilDate = new Date();
        untilDate.setTime(fromDate.getTime() + 1000);
        System.out.println("Listing one oai_dc record");
        iter = m_impl.listRecords(fromDate, untilDate, OAI_DC_PREFIX);
        checkRecords(iter, new int[]{2});
    }

    @Test
    public void listingZeroTestFormatRecords() throws Exception {
        Iterator<? extends Record> iter;
        Date fromDate = null;

        Date untilDate = new Date();
        untilDate.setTime(1);

        System.out.println("Listing zero test_form records");
        iter = m_impl.listRecords(fromDate, untilDate, TEST_FORMAT_PREFIX);
        checkRecords(iter, new int[]{});
    }

    @Test
    public void listingZeroOaiDcRecords() throws Exception {
        Iterator<? extends Record> iter;
        Date fromDate = null;
        Date untilDate = new Date();
        untilDate.setTime(1);

        System.out.println("Listing zero oai_dc records");
        iter = m_impl.listRecords(fromDate, untilDate, OAI_DC_PREFIX);
        checkRecords(iter, new int[]{});
    }

    @Test
    public void listingAllTestFormatRecords() throws Exception {
        Iterator<? extends Record> iter;
        Date fromDate = null;
        Date untilDate = m_impl.getLatestDate();

        System.out.println("Listing all test_form records");
        iter = m_impl.listRecords(fromDate, untilDate, TEST_FORMAT_PREFIX);
        checkRecords(iter, new int[]{1, 2, 3});
    }

    @Test
    public void listAllOaiDcRecords() throws Exception {
        Iterator<? extends Record> iter;
        Date fromDate = null;
        Date untilDate = m_impl.getLatestDate();

        System.out.println("Listing all oai_dc records");
        iter = m_impl.listRecords(fromDate, untilDate, OAI_DC_PREFIX);
        checkRecords(iter, new int[]{1, 2, 3, 4, 5});
    }

}
