package proai.cache;

import org.apache.log4j.Logger;
import proai.CloseableIterator;
import proai.error.ServerException;

import java.sql.*;
import java.text.SimpleDateFormat;

/**
 * An iterator around a database <code>ResultSet</code> that provides
 * a <code>String[]</code> for each row.
 * <p/>
 * Rows in the result set contain two values.  The first value is a
 * <code>String</code> representing a relative filesystem path.  The second
 * value is a <code>long</code> representing a date.
 * <p/>
 * The returned <code>String[]</code> for each row will have two parts:
 * The first is the relative filesystem path and the second is an
 * ISO8601-formatted date (second precision).
 */
public class StringResultIterator implements CloseableIterator<String[]> {

    private static final Logger logger =
            Logger.getLogger(StringResultIterator.class.getName());

    private Connection m_conn;
    private Statement m_stmt;
    private ResultSet m_rs;

    private boolean m_closed;

    private String[] m_nextStringArray;

    private boolean m_exhausted;

    public StringResultIterator(Connection conn,
                                Statement stmt,
                                ResultSet rs) throws ServerException {
        logger.debug("Constructing");
        m_conn = conn;
        m_stmt = stmt;
        m_rs = rs;
        m_closed = false;
        m_nextStringArray = getNext();
    }

    private String[] getNext() throws ServerException {
        if (m_exhausted) return null;
        try {
            if (m_rs.next()) {
                String[] result = new String[2];
                result[0] = m_rs.getString(1);
                Date d = new Date(m_rs.getLong(2));
                try {
                    result[1] = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(d);
                } catch (Exception e) { // won't happen
                    e.printStackTrace();
                }
                return result;
            } else {
                m_exhausted = true;
                close(); // since we know it was exhausted
                return null;
            }
        } catch (SQLException e) {
            close(); // since we know there was an error
            throw new ServerException("Error pre-getting next string from db", e);
        }
    }

    public boolean hasNext() {
        return m_nextStringArray != null;
    }

    public String[] next() throws ServerException {
        String[] next = m_nextStringArray;
        m_nextStringArray = getNext();
        return next;
    }

    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("StringResultIterator does not support remove().");
    }

    public void close() {
        if (!m_closed) {
            if (m_rs != null) try {
                m_rs.close();
                m_rs = null;
            } catch (Exception e) {
            }
            if (m_stmt != null) try {
                m_stmt.close();
                m_stmt = null;
            } catch (Exception e) {
            }
            RecordCache.releaseConnection(m_conn);

            // gc and print memory stats when we're done with the
            // (potentially large) resultset.
            long startTime = System.currentTimeMillis();
            long startFreeBytes = Runtime.getRuntime().freeMemory();
            System.gc();
            long ms = System.currentTimeMillis() - startTime;
            long currentFreeBytes = Runtime.getRuntime().freeMemory();
            logger.info("GC ran in " + ms + "ms and free memory "
                    + "went from " + startFreeBytes + " to "
                    + currentFreeBytes + " bytes.");

            m_closed = true;
            logger.info("Closed.");
        }
    }

    public void finalize() {
        close();
    }

}