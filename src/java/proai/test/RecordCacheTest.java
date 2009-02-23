package proai.test;

import junit.framework.*;

import proai.cache.*;

public class RecordCacheTest extends TestCase {

    private RecordCache m_cache;

    public void setUp() {
        m_cache = new RecordCache(System.getProperties());
    }

    ///////////////////////////////////////////////////////////////////////////

    public void testUpdate() throws Exception {
        //m_cache.update();
        try { Thread.sleep(2000); } catch (Exception e) { }
    }

    ///////////////////////////////////////////////////////////////////////////

    public void tearDown() {
        m_cache.close();
    }

	public RecordCacheTest(String name) { super (name); }
	
	public static void main(String[] args) {
		junit.textui.TestRunner.run(RecordCacheTest.class);
	}

}
