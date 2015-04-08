package proai.service;

import org.apache.log4j.Logger;
import proai.CloseableIterator;
import proai.Writable;
import proai.cache.CachedContent;
import proai.error.BadResumptionTokenException;
import proai.error.ServerException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Date;

public class SnapshotSession<T> extends Thread
        implements Session {

    private static final Logger logger =
            Logger.getLogger(SnapshotSession.class.getName());

    private SessionManager m_manager;
    private File m_baseDir;
    private int m_secondsBetweenRequests;
    private ListProvider<T> m_provider;

    private String m_sessionKey;

    private int m_threadWorkingPart;
    private int m_lastGeneratedPart;
    private int m_lastSentPart;
    private long m_expirationTime;
    private ServerException m_exception;

    private boolean m_threadNeedsToFinish;
    private boolean m_threadWorking;

    public SnapshotSession(SessionManager manager,
                           File baseDir,
                           int secondsBetweenRequests,
                           ListProvider<T> provider) {
        m_manager = manager;
        m_baseDir = baseDir;
        m_secondsBetweenRequests = secondsBetweenRequests;
        m_provider = provider;

        // make a unique key for this session
        String s = "" + this.hashCode();
        if (s.startsWith("-")) {
            m_sessionKey = "Z" + s.substring(1);
        } else {
            m_sessionKey = "X" + s;
        }

        m_threadWorkingPart = 0;
        m_lastGeneratedPart = -1;
        m_lastSentPart = -1;

        m_threadWorking = true;
        start();
    }

    ///////////////////////////////////////////////////////////////////////////

    public void run() {
        logger.info(m_sessionKey + " retrieval thread started");
        int incompleteListSize = m_provider.getIncompleteListSize();
        CloseableIterator<T> iter = null;
        PrintWriter out = null;
        try {
            iter = m_provider.getList();  // if empty, the impl should throw the right exception here
            m_manager.addSession(m_sessionKey, this);  // after this point, we depend on the session manager to clean up

            File sessionDir = new File(m_baseDir, m_sessionKey);
            sessionDir.mkdirs();

            int cursor = 0;

            // iterate all elements of the list...
            while (iter.hasNext() && !m_threadNeedsToFinish) {
                File partFile = new File(sessionDir, m_threadWorkingPart + ".xml");
                out = new PrintWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(partFile), "UTF-8"));
                out.println("<" + m_provider.getVerb() + ">");
                for (int i = 0; i < incompleteListSize && iter.hasNext(); i++) {
                    Writable writable = (Writable) iter.next();
                    writable.write(out);
                }

                if (iter.hasNext()) {
                    int nextPartNum = m_threadWorkingPart + 1;
                    String token = m_sessionKey + "/" + nextPartNum;
                    out.println("<resumptionToken cursor=\"" + cursor + "\">" + token + "</resumptionToken>");
                    cursor += incompleteListSize;
                } else if (cursor > 0) {
                    out.println("<resumptionToken cursor=\"" + cursor + "\"/>");
                }
                out.println("</" + m_provider.getVerb() + ">");
                out.close();
                m_lastGeneratedPart++;
                if (iter.hasNext()) {
                    m_threadWorkingPart++;
                }
            }
        } catch (ServerException e) {
            m_exception = e;
        } catch (Throwable th) {
            m_exception = new ServerException("Unexpected error in session thread", th);
        } finally {
            if (iter != null) try {
                iter.close();
            } catch (Exception e) {
            }
            if (out != null) try {
                out.close();
            } catch (Exception e) {
            }
            m_threadWorking = false;
            logger.info(m_sessionKey + " retrieval thread finished");
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * Has the session expired?
     * <p/>
     * If this is true, the session will be cleaned by the reaper thread
     * of the session manager.
     */
    public boolean hasExpired() {
        if (m_exception != null) return true;
        if (m_lastGeneratedPart >= 0) {
            return m_expirationTime < System.currentTimeMillis();
        }
        return false;
    }

    /**
     * Do all possible cleanup for this session.
     * <p/>
     * This includes signaling to its thread to stop asap,
     * waiting for it to stop, and removing any files/directories that remain.
     * <p/>
     * The implementation should be fail-safe, as a session may be asked to
     * clean itself more than once.
     * <p/>
     * Clean must *not* be called from this session's thread.
     */
    public void clean() {
        m_threadNeedsToFinish = true;
        while (m_threadWorking) {
            try {
                Thread.sleep(250);
            } catch (Exception e) {
            }
        }
        File sessionDir = new File(m_baseDir, m_sessionKey);
        File[] files = sessionDir.listFiles();
        for (int i = 0; i < files.length; i++) {
            files[i].delete();
        }
        sessionDir.delete();
    }

    /**
     * Get the named response, wait for it, or throw any exception that
     * has popped up while generating parts.
     */
    public ResponseData getResponseData(int partNum) throws ServerException {
        if (m_exception != null) {
            throw m_exception;
        }

        // the current request should either be for the last sent part or plus one
        int nextPart = m_lastSentPart + 1;
        if (partNum == m_lastSentPart || partNum == nextPart) {

            // If the thread is still running and the last generated part was less than partNum,
            // wait till the thread is finished or the last generated part is greater or equal to partNum

            // Then, try to return the response
            while (m_threadWorking && m_lastGeneratedPart < partNum) {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                }
            }
            if (m_exception != null) {
                throw m_exception;
            }
            File partFile = new File(m_baseDir, m_sessionKey + "/" + partNum + ".xml");
            if (!partFile.exists()) {
                throw new BadResumptionTokenException("the indicated part does not exist");
            }
            String token = getResumptionToken(partNum + 1);
            ResponseData response = new ResponseDataImpl(new CachedContent(partFile), token);

            // Since we're going to succeed, make sure we clean up the previous part, if any
            if (partNum > 0) {
                int previousPart = partNum - 1;
                new File(m_baseDir, m_sessionKey + "/" + previousPart + ".xml").delete();
            }

            m_lastSentPart = partNum;
            m_expirationTime = new Date().getTime() + (1000 * m_secondsBetweenRequests);
            logger.info(m_sessionKey + " returning part " + partNum);
            return response;
        } else {
            throw new BadResumptionTokenException("the indicated part either doesn't exist yet or has expired");
        }
    }

    private String getResumptionToken(int partNum) {
        if (m_threadWorking) {
            if (m_threadWorkingPart >= partNum) {
                return m_sessionKey + "/" + partNum;
            } else {
                return null; // assume if it's not working on it or hasn't yet, there is no such part
            }
        } else {
            if (partNum <= m_lastGeneratedPart) {
                return m_sessionKey + "/" + partNum;
            } else {
                return null;
            }
        }
    }


    public void finalize() {
        clean();
    }

}