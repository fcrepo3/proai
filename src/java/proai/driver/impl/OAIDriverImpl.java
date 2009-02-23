package proai.driver.impl;

import java.io.*;
import java.text.*;
import java.util.*;

import proai.MetadataFormat;
import proai.Record;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

/**
 * An simple OAIDriver for testing/demonstration purposes.
 *
 * The directory should contain the following files:
 *
 * identity.xml
 * records/
 *   item1-oai_dc-2005-01-01T08-50-44.xml
 * sets/
 *   abovetwo.xml
 *   abovetwo-even.xml
 *   abovetwo-odd.xml
 *   prime.xml
 * formats/
 *   oai_dc.txt
 *     line1: ns
 *     line2: loc
 */
public class OAIDriverImpl implements OAIDriver {

    public static final String BASE_DIR_PROPERTY = "proai.driver.simple.baseDir";

    public static final String IDENTITY_FILENAME = "identity.xml";
    public static final String RECORDS_DIRNAME   = "records";
    public static final String SETS_DIRNAME      = "sets";
    public static final String FORMATS_DIRNAME   = "formats";

    private File m_identityFile;
    private File m_recordsDir;
    private File m_setsDir;
    private File m_formatsDir;

    public OAIDriverImpl() {
    }

    public OAIDriverImpl(File dir) throws RepositoryException {
        Properties props = new Properties();
        props.setProperty(BASE_DIR_PROPERTY, dir.getPath());
        init(props);
    }

    public void init(Properties props) throws RepositoryException {
        String baseDir = props.getProperty(BASE_DIR_PROPERTY);
        if (baseDir == null) {
            throw new RepositoryException("Required property is not set: " 
                    + BASE_DIR_PROPERTY);
        }
        File dir = new File(baseDir);
        m_identityFile = new File(dir, IDENTITY_FILENAME);
        m_recordsDir   = new File(dir, RECORDS_DIRNAME);
        m_setsDir      = new File(dir, SETS_DIRNAME);
        m_formatsDir   = new File(dir, FORMATS_DIRNAME);
        if (!dir.exists()) {
            throw new RepositoryException("Base directory does not exist: " 
                    + dir.getPath());
        }
        if (!m_identityFile.exists()) {
            throw new RepositoryException("Identity file does not exist: " 
                    + m_identityFile.getPath());
        }
        if (!m_recordsDir.exists()) {
            throw new RepositoryException("Records directory does not exist: " 
                    + m_recordsDir.getPath());
        }
        if (!m_setsDir.exists()) {
            throw new RepositoryException("Sets directory does not exist: " 
                    + m_setsDir.getPath());
        }
        if (!m_formatsDir.exists()) {
            throw new RepositoryException("Formats directory does not exist: " 
                    + m_formatsDir.getPath());
        }
    }

    public void write(PrintWriter out) throws RepositoryException {
        writeFromFile(m_identityFile, out);
    }

    public Date getLatestDate() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");
        long latest = 0;
        String[] names = m_recordsDir.list();
        for (int i = 0; i < names.length; i++) {
            String[] temp = names[i].replaceFirst("-", " ")
                                    .replaceFirst("-", " ")
                                    .split(" ");
            if (temp.length == 3 && temp[2].indexOf(".") != -1) {
                try {
                    long recDate = df.parse(temp[2].substring(0, temp[2].indexOf("."))).getTime();
                    if (recDate > latest) latest = recDate;
                } catch (Exception e) { 
                    System.out.println("WARNING: Ignoring unparsable filename: " 
                                       + names[i]);
                }
            }
        }
        return new Date(latest);
    }

    public RemoteIterator<MetadataFormat> listMetadataFormats() {
        return new RemoteIteratorImpl<MetadataFormat>(
        		getMetadataFormatCollection().iterator());
    }

    public RemoteIterator<SetInfo> listSetInfo() {
        return new RemoteIteratorImpl<SetInfo>(
        		getSetInfoCollection().iterator());
    }

    public RemoteIterator<Record> listRecords(Date from, 
                                      Date until, 
                                      String mdPrefix) {
        return new RemoteIteratorImpl<Record>(getRecordCollection(from,
                                                          until,
                                                          mdPrefix).iterator());
    }

    // In this case, sourceInfo is the full path to the source file.
    public void writeRecordXML(String itemID,
                               String mdPrefix,
                               String sourceInfo,
                               PrintWriter writer) throws RepositoryException {

        File file = new File(sourceInfo);
        writeFromFile(file, writer);
    }

    public void close() {
        // do nothing (this impl doesn't tie up any resources)
    }

    public static void writeFromFile(File file, 
                                     PrintWriter out) throws RepositoryException {
        try {
            BufferedReader reader = new BufferedReader(
                                        new InputStreamReader(
                                            new FileInputStream(file), "UTF-8"));
            String line = reader.readLine();
            while (line != null) {
                out.println(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            throw new RepositoryException("Error reading from file: " + file.getPath(), e);
        }
    }

    private Collection<SetInfo> getSetInfoCollection() {
        try {
            List<SetInfo> list = new ArrayList<SetInfo>();
            String[] names = m_setsDir.list();
            for (int i = 0; i < names.length; i++) {
                if (names[i].endsWith(".xml")) {
                    String spec = names[i].split("\\.")[0].replaceAll("-", ":");
                    list.add(new SetInfoImpl(spec, new File(m_setsDir, 
                                                            names[i])));
                }
            }
            return list;
        } catch (Exception e) {
            throw new RepositoryException("Error getting set information", e);
        }
    }

    private Collection<MetadataFormat> getMetadataFormatCollection() {
        try {
            List<MetadataFormat> list = new ArrayList<MetadataFormat>();
            String[] names = m_formatsDir.list();
            for (int i = 0; i < names.length; i++) {
                if (names[i].endsWith(".txt")) {
                    String prefix = names[i].split("\\.")[0];
                    BufferedReader reader = 
                            new BufferedReader(
                                new InputStreamReader(
                                    new FileInputStream(new File(m_formatsDir, names[i])),
                                    "UTF-8"));
                    String uri = reader.readLine();
                    if (uri == null) {
                        throw new RepositoryException("Error reading first "
                                + "line of format file: " + names[i]);
                    }
                    String loc = reader.readLine();
                    if (loc == null) {
                        throw new RepositoryException("Error reading second "
                                + "line of format file: " + names[i]);
                    }
                    list.add(new MetadataFormatImpl(prefix, uri, loc));
                }
            }
            return list;
        } catch (Exception e) {
            throw new RepositoryException("Error getting metadata formats", e);
        }
    }

    private Collection<Record> getRecordCollection(Date from, 
                                           Date until, 
                                           String mdPrefix) {
        List<Record> list = new ArrayList<Record>();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");
        String[] names = m_recordsDir.list();
        for (int i = 0; i < names.length; i++) {
            String[] temp = names[i].replaceFirst("-", " ")
                                    .replaceFirst("-", " ")
                                    .split(" ");
            if (temp.length == 3 && temp[2].indexOf(".") != -1) {
                String[] parts = names[i].split("-");
                if (parts[1].equals(mdPrefix)) {
                    try {
                        long recDate = df.parse(temp[2].substring(0, temp[2].indexOf("."))).getTime();
                        if ( (from == null || from.getTime() < recDate)
                                && (until.getTime() >= recDate) ) {
                            String itemID = "oai:example.org:" + parts[0];
                            list.add(new RecordImpl(itemID,
                                                    mdPrefix,
                                                    new File(m_recordsDir,
                                                             names[i])));
                        }
                    } catch (Exception e) { 
                        System.out.println("WARNING: Ignoring unparsable filename: " 
                                           + names[i]);
                    }
                }
            }
        }
        return list;
    }

}
