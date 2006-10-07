package proai.test;

import java.io.*;
import java.text.*;
import java.util.*;

import junit.framework.*;
import junit.extensions.*;

import proai.*;
import proai.driver.*;
import proai.driver.impl.*;

public class OAIDriverImplTest extends TestCase {

    public static final String LATEST_DATE         = "2005-01-01T08:57:59Z";

    public static final String OAI_DC_PREFIX       = "oai_dc";
    public static final String OAI_DC_URI          = "http://www.openarchives.org/OAI/2.0/oai_dc/";
    public static final String OAI_DC_LOC          = "http://www.openarchives.org/OAI/2.0/oai_dc.xsd";
    public static final String TEST_FORMAT_PREFIX  = "test_format";
    public static final String TEST_FORMAT_URI     = "http://example.org/testFormat/";
    public static final String TEST_FORMAT_LOC     = "http://example.org/testFormat.xsd";

    public static final String ABOVE_TWO_SPEC      = "abovetwo";
    public static final String ABOVE_TWO_EVEN_SPEC = "abovetwo:even";
    public static final String ABOVE_TWO_ODD_SPEC  = "abovetwo:odd";
    public static final String PRIME_SPEC          = "prime";

    public static final String ITEM1               = "oai:example.org:item1";
    public static final String ITEM2               = "oai:example.org:item2";
    public static final String ITEM3               = "oai:example.org:item3";
    public static final String ITEM4               = "oai:example.org:item4";

    private OAIDriver m_impl;

    public void setUp() {
        m_impl = new OAIDriverImpl();
        m_impl.init(System.getProperties());
    }

    ///////////////////////////////////////////////////////////////////////////

    public void testLatestDate() throws Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String latestDate = df.format(m_impl.getLatestDate());
        System.out.println("Latest Date was " + latestDate);
        assertEquals(latestDate, LATEST_DATE);
    }

    public void testIdentity() throws Exception {
        StringWriter writer = new StringWriter();
        m_impl.write(new PrintWriter(writer, true));
        System.out.println("Result of writeIdentity:\n" + writer.toString());
    }

    public void testFormats() throws Exception {
        Iterator iter = m_impl.listMetadataFormats();
        while (iter.hasNext()) {
            MetadataFormat format = (MetadataFormat) iter.next();
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

    public void testSets() throws Exception {
        Iterator iter = m_impl.listSetInfo();
        while (iter.hasNext()) {
            SetInfo info = (SetInfo) iter.next();
            String spec = info.getSetSpec();
            System.out.println("Set spec = " + spec);
            assertTrue(spec.equals(ABOVE_TWO_SPEC) 
                    || spec.equals(ABOVE_TWO_EVEN_SPEC)
                    || spec.equals(ABOVE_TWO_ODD_SPEC)
                    || spec.equals(PRIME_SPEC));
            StringWriter writer = new StringWriter();
            info.write(new PrintWriter(writer, true));
            System.out.println("Result of write: \n" + writer.toString());
        }
    }

    public void testRecords() throws Exception {
        Iterator iter;
        Date fromDate = null;
        Date untilDate = m_impl.getLatestDate();

        System.out.println("Listing all oai_dc records");
        iter = m_impl.listRecords(fromDate, untilDate, OAI_DC_PREFIX);
        checkRecords(iter, new int[] {1, 2, 3, 4});

        System.out.println("Listing all test_form records");
        iter = m_impl.listRecords(fromDate, untilDate, TEST_FORMAT_PREFIX);
        checkRecords(iter, new int[] {1, 2, 3});

        untilDate = new Date();
        untilDate.setTime(1);

        System.out.println("Listing zero oai_dc records");
        iter = m_impl.listRecords(fromDate, untilDate, OAI_DC_PREFIX);
        checkRecords(iter, new int[] {});

        System.out.println("Listing zero test_form records");
        iter = m_impl.listRecords(fromDate, untilDate, TEST_FORMAT_PREFIX);
        checkRecords(iter, new int[] {});

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        fromDate = df.parse("2005-01-01T08:52:20Z");
        untilDate = new Date();
        untilDate.setTime(fromDate.getTime() + 1000);

        System.out.println("Listing one oai_dc record");
        iter = m_impl.listRecords(fromDate, untilDate, OAI_DC_PREFIX);
        checkRecords(iter, new int[] {2});

        System.out.println("Listing one test_format record");
        iter = m_impl.listRecords(fromDate, untilDate, TEST_FORMAT_PREFIX);
        checkRecords(iter, new int[] {2});

        System.out.println("Listing zero unknown_format records");
        iter = m_impl.listRecords(fromDate, untilDate, "unknown_format");
        checkRecords(iter, new int[] {});

    }

    public void checkRecords(Iterator iter, int[] expecting) throws Exception {
        boolean[] saw = new boolean[expecting.length]; 
        while (iter.hasNext()) {
            Record rec = (Record) iter.next();
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
            StringWriter writer = new StringWriter();
            ((Writable) rec).write(new PrintWriter(writer, true));
            System.out.println("Result of write: \n" + writer.toString());
        }
        for (int i = 0; i < saw.length; i++) {
            assertTrue(saw[i]);
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    public void tearDown() {
        m_impl.close();
    }

	public OAIDriverImplTest(String name) { super (name); }
	
	public static void main(String[] args) {
		junit.textui.TestRunner.run(OAIDriverImplTest.class);
	}

}
