package proai.cache;

import java.io.*;
import java.text.*;
import java.util.*;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import proai.Record;
import proai.error.ServerException;
import proai.util.StreamUtil;

public class ParsedRecord extends DefaultHandler implements Record {

    private static final Logger _LOG =
            Logger.getLogger(ParsedRecord.class.getName());

    private String m_itemID;
    private String m_prefix;
    private String m_sourceInfo;

    private File m_file;
    private Date m_date;
    private List<String> m_setSpecs;

    private SimpleDateFormat m_formatter1;
    private SimpleDateFormat m_formatter2;
    
    private boolean m_inDatestamp;
    private boolean m_inSetSpec;
    private boolean m_finishedParsing;
    private StringBuffer m_buf = null;

    public ParsedRecord(String itemID, 
                        String prefix, 
                        String sourceInfo,
                        File file) throws ServerException {
        m_itemID = itemID;
        m_prefix = prefix;
        m_sourceInfo = sourceInfo;
        m_file = file;
        m_date = new Date(0);
        m_setSpecs = new ArrayList<String>();

        m_formatter1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        m_formatter2 = new SimpleDateFormat("yyyy-MM-dd");

        m_inDatestamp = false;
        m_inSetSpec = false;
        m_finishedParsing = false;

        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(false);
            SAXParser parser = spf.newSAXParser();
            parser.parse(new FileInputStream(file), this);
        } catch (Exception e) {
            if (_LOG.isDebugEnabled() && file.exists()) {
                try {
                    String xml = StreamUtil.getString(new FileInputStream(file), "UTF-8");
                    _LOG.debug("Error parsing record xml: #BEGIN-XML#" + xml + "#END-XML#");
                } catch (Exception ex) { }
            }
            throw new ServerException("Error parsing record xml", e);
        }
    }

    public void startElement(String uri, 
                             String localName, 
                             String qName, 
                             Attributes a) throws SAXException {
        if (!m_finishedParsing) {
            if (qName.equals("datestamp")) {
                m_inDatestamp = true;
                m_buf = new StringBuffer();
            } else if (qName.equals("setSpec")) {
                m_inSetSpec = true;
                m_buf = new StringBuffer();
            }
        }
    }

    public void characters(char[] ch, int start, int length) throws SAXException {
        if (!m_finishedParsing) {
            if (m_inDatestamp || m_inSetSpec) {
                m_buf.append(ch, start, length);
            }
        }
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (!m_finishedParsing) {
            if (qName.equals("header")) {
                m_finishedParsing = true;
            } else if (qName.equals("datestamp")) {
                String s = m_buf.toString().trim();
                try {
                    try {
                        m_date = m_formatter1.parse(s);
                    } catch (Exception e) {
                        m_date = m_formatter2.parse(s);
                    }
                } catch (Exception e) {
                    throw new SAXException("Record datestamp is unparsable: " + s);
                }
                m_inDatestamp = false;
            } else if (qName.equals("setSpec")) {
                String s = m_buf.toString().trim();
                // Infer memberships based on setSpec:syntax:stuff
                String[] h = s.split(":");
                if (h.length > 1) {
                    StringBuffer b4 = new StringBuffer();
                    for (int i = 0; i < h.length; i++) {
                        m_setSpecs.add(b4.toString() + h[i]);
                        b4.append(h[i] + ":");
                    }
                } else {
                    m_setSpecs.add(m_buf.toString().trim());
                }
                m_inSetSpec = false;
            }
        }
    }

    // From Record interface
    public String getItemID() {
        return m_itemID;
    }

    // From Record interface
    public String getPrefix() {
        return m_prefix;
    }

    // From Record interface
    // In this case, the sourceInfo is the path to the xml inside
    // the cache, relative to rcDisk's baseDir.
    public String getSourceInfo() {
        return m_sourceInfo;
    }

    public Date getDate() {
        return m_date;
    }

    public List<String> getSetSpecs() {
        return m_setSpecs;
    }

    public boolean deleteFile() {
        return m_file.delete();
    }

}
