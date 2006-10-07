package proai.cache;

// FIXME: This probably goes in the proai.service package

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import proai.Writable;
import proai.error.ServerException;

public class CachedContentAggregate implements Writable {

    private File m_listFile;
    private String m_verb;
    private RecordCache m_cache;

    /**
     * Expects a file of the following form.
     *
     * line 0 - n : path [dateString if record or header]
     * line n+1   : "end" or "end resumptionToken cursor"
     *
     * Note that if dateString isn't given for a line in the file, it will 
     * be written as-is.
     */
    public CachedContentAggregate(File listFile, 
                                  String verb,
                                  RecordCache cache) {
        m_listFile = listFile;
        m_verb = verb;
        m_cache = cache;
    }

    public void write(PrintWriter out) throws ServerException {
        BufferedReader lineReader = null;
        try {
            boolean headersOnly = m_verb.equals("ListIdentifiers");
            out.println("<" + m_verb + ">");
            lineReader = new BufferedReader(
                             new InputStreamReader(
                                 new FileInputStream(m_listFile), "UTF-8"));
            String line = lineReader.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                if (line.startsWith("end")) {
                    if (parts.length == 3) {
                        // it's resumable so write the resumptionToken w/cursor
                        out.println("<resumptionToken cursor=\"" + parts[2] + "\">" + parts[1] + "</resumptionToken>");
                    } else if (parts.length == 2) {
                        // it's the last part so write an empty resumptionToken w/cursor
                        out.println("<resumptionToken cursor=\"" + parts[1] + "\"/>");
                    }
                    line = null;
                } else {
                    try {
                        // if it has two parts, we assume the date should be
                        // translated to the one given, else the content
                        // is given as-is.
                        if (parts.length == 2) {
                            new CachedContent(m_cache.getFile(parts[0]), 
                                              parts[1], 
                                              headersOnly).write(out);
                        } else {
                            new CachedContent(m_cache.getFile(parts[0])).write(out);
                        }
                    } catch (Exception e) {
                        // must have moved out of cache -- ignore
                    }
                    line = lineReader.readLine();
                }
            }
            out.println("</" + m_verb + ">");
        } catch (Exception e) {
            throw new ServerException("Error writing cached content aggregate", e);
        } finally {
            if (lineReader != null) try { lineReader.close(); } catch (Exception e) { }
        }
    }

}