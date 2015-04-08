package proai.cache;

import proai.Writable;
import proai.error.ServerException;

import java.io.*;

public class CachedContent implements Writable {

    private File m_file;

    private String m_dateStamp;
    private boolean m_headerOnly;

    private String m_string;

    public CachedContent(File file) {
        m_file = file;
    }

    public CachedContent(File file, String dateStamp, boolean headerOnly) {
        m_file = file;
        m_dateStamp = dateStamp;
        m_headerOnly = headerOnly;
    }

    public CachedContent(String content) {
        m_string = content;
    }

    public void write(PrintWriter out) throws ServerException {
        if (m_file != null) {
            if (m_dateStamp == null) {
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(
                            new InputStreamReader(
                                    new FileInputStream(m_file), "UTF-8"));
                    String line = reader.readLine();
                    while (line != null) {
                        out.println(line);
                        line = reader.readLine();
                    }
                } catch (Exception e) {
                    throw new ServerException("Error reading from file: " + m_file.getPath(), e);
                } finally {
                    if (reader != null) try {
                        reader.close();
                    } catch (Exception e) {
                    }
                }
            } else {
                // need to read the file while changing the <datestamp>,
                // and only output things inside <header> if m_headerOnly
                writeChanged(out);
            }
        } else {
            out.println(m_string);
        }
    }

    private void writeChanged(PrintWriter out) throws ServerException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(m_file), "UTF-8"));
            StringBuffer upToHeaderEnd = new StringBuffer();
            String line = reader.readLine();
            boolean sawHeaderEnd = false;
            while (line != null && !sawHeaderEnd) {
                upToHeaderEnd.append(line + "\n");
                if (line.indexOf("</h") != -1) {
                    sawHeaderEnd = true;
                }
                line = reader.readLine();
            }
            if (!sawHeaderEnd) throw new ServerException("While parsing, never saw </header>");
            String fixed = upToHeaderEnd.toString().replaceFirst("p>[^<]+<", "p>" + m_dateStamp + "<");

            if (m_headerOnly) {
                int headerStart = fixed.indexOf("<h");
                if (headerStart == -1) throw new ServerException("While parsing, never saw <header...");
                fixed = fixed.substring(headerStart);
                int headerEnd = fixed.indexOf("</h"); // we already know this exists
                fixed = fixed.substring(0, headerEnd) + "</header>";
                out.println(fixed);
            } else {
                out.print(fixed);
                out.println(line);
                line = reader.readLine();
                while (line != null) {
                    out.println(line);
                    line = reader.readLine();
                }
            }
        } catch (Exception e) {
            throw new ServerException("Error reading/transforming file: " + m_file.getPath(), e);
        } finally {
            if (reader != null) try {
                reader.close();
            } catch (Exception e) {
            }
        }

    }

}