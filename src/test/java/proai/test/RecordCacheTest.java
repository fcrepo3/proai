package proai.test;


import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import proai.cache.RecordCache;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class RecordCacheTest {

    private RecordCache m_cache;

    @Before
    public void setUp() throws IOException {
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/proai.properties"));

        File baseDir = new File(getClass().getResource("/").getFile());
        File cacheDir = new File(baseDir, "cache");
        cacheDir.mkdir();

        properties.setProperty("proai.cacheBaseDir", cacheDir.getAbsolutePath());

        m_cache = new RecordCache(properties);
    }

    @After
    public void tearDown() {
        m_cache.close();
    }

    @Test
    @Ignore("Test is not doing anything")
    public void testUpdate() throws Exception {
        // TODO change test or remove it
        //m_cache.update();
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
        }
    }

}
