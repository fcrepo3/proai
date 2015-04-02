package proai.cache;

import java.io.*;

public class RCDiskWriter extends PrintWriter {

    private String m_path;
    private File m_file;

    public RCDiskWriter(File baseDir, String path) throws Exception {
        super(new FileOutputStream(new File(baseDir, path)));
        m_path = path;
        m_file = new File(baseDir, path);
    }

    public String getPath() {
        return m_path;
    }

    public File getFile() {
        return m_file;
    }

}