package proai.service;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import proai.error.BadResumptionTokenException;
import proai.error.ServerException;

public class SessionManager extends Thread {

    private static final Logger logger =
            Logger.getLogger(SessionManager.class.getName());

    public static final String PROP_BASEDIR = "proai.sessionBaseDir";
    public static final String PROP_SECONDSBETWEENREQUESTS = "proai.secondsBetweenRequests";

    public static final String ERR_RESUMPTION_SYNTAX_SLASH = "bad syntax in resumption token: must contain exactly one slash";
    public static final String ERR_RESUMPTION_SYNTAX_INTEGER = "bad syntax in resumption token: expected an integer after the slash";
    public static final String ERR_RESUMPTION_SESSION = "bad session id or session expired";

    private File m_baseDir;
    private int m_secondsBetweenRequests;

    private Map<String, Session> m_sessions;
    private boolean m_threadNeedsToFinish;
    private boolean m_threadFinished;

    public SessionManager(Properties props) throws ServerException {
        String dir = props.getProperty(PROP_BASEDIR);
        if (dir == null) throw new ServerException("Required property missing: " + PROP_BASEDIR);
        String sec = props.getProperty(PROP_SECONDSBETWEENREQUESTS);
        if (sec == null) throw new ServerException("Required property missing: " + PROP_SECONDSBETWEENREQUESTS);
        int secondsBetweenRequests;
        try {
            secondsBetweenRequests = Integer.parseInt(sec);
        } catch (Exception e) {
            throw new ServerException("Required property must an integer: " + PROP_SECONDSBETWEENREQUESTS);
        }
        init(new File(dir), secondsBetweenRequests);
    }

    public SessionManager(File baseDir, int secondsBetweenRequests) {
        init(baseDir, secondsBetweenRequests);
    }

    private void init(File baseDir, int secondsBetweenRequests) throws ServerException {
        m_baseDir = baseDir;
        m_baseDir.mkdirs();
        File[] dirs = m_baseDir.listFiles();
        if (dirs == null) throw new ServerException("Unable to create session directory: " + m_baseDir.getPath());
        if (dirs.length > 0) {
            logger.info("Cleaning up " + dirs.length + " sessions from last run...");
            try { Thread.sleep(4000); } catch (Exception e) { }
            for (int i = 0; i < dirs.length; i++) {
                if (dirs[i].isDirectory()) {
                    File[] files = dirs[i].listFiles();
                    for (int j = 0; j < files.length; j++) {
                        files[j].delete();
                    }
                }
                dirs[i].delete();
            }
        }

        m_secondsBetweenRequests = secondsBetweenRequests;
        m_sessions = new HashMap<String, Session>();
        setName("Session-Reaper");
        start();
    }

    //////////////////////////////////////////////////////////////////////////

    /**
     * Session timeout reaper thread.
     */
    public void run() {
        // each session has an associated thread which will stop as soon as it 
        // finishes iterating.  The purpose of *this* thread is to 
        // 1) removed the session from the map and 
        // 2) make sure any files created by sessions are cleaned up

        while (!m_threadNeedsToFinish) {
            cleanupSessions(false);
            int c = 0;
            while (c < 20 && !m_threadNeedsToFinish) {
                c++;
                try { Thread.sleep(250); } catch (Exception e) { }
            }

        }
        m_threadFinished = true;
    }

    /**
     * If force is false, clean up expired sessions.
     * If force is true, clean up all sessions.
     */
    private void cleanupSessions(boolean force) {
        List<String> toCleanKeys = new ArrayList<String>();
        List<Session> toCleanSessions = new ArrayList<Session>();
        synchronized (m_sessions) {
            Iterator<String> iter = m_sessions.keySet().iterator();
            while (iter.hasNext()) {
                String key = iter.next();
                Session sess = m_sessions.get(key);
                if (force || sess.hasExpired()) {
                    toCleanKeys.add(key);
                    toCleanSessions.add(sess);
                }
            }
            if (toCleanKeys.size() > 0) {
                String dueTo;
                if (force) {
                    dueTo = "shutdown)";
                } else {
                    dueTo = "expired)";
                }
                logger.info("Cleaning up " + toCleanKeys.size() + " sessions (" + dueTo);
                for (int i = 0; i < toCleanKeys.size(); i++) {
                    ((Session) toCleanSessions.get(i)).clean();
                    m_sessions.remove((String) toCleanKeys.get(i));
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////

    /**
     * Add a session to the map of tracked sessions.
     * This is called by any session that has multiple responses.
     */
    protected void addSession(String key, Session session) {
        if (m_threadNeedsToFinish) {
            session.clean();
        } else {
            synchronized (m_sessions) {
                m_sessions.put(key, session);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////

    public <T> ResponseData list(ListProvider<T> provider) throws ServerException {
        // Session session = new SnapshotSession(this, m_baseDir, m_secondsBetweenRequests, provider);
        Session session = new CacheSession<T>(this, m_baseDir, m_secondsBetweenRequests, provider);
        return session.getResponseData(0);
    }

    /**
     * Get response data from the appropriate session and return it.
     *
     * The resumption token encodes the session id and part number.
     * The first part is sessionid/0, the second part is sessionid/1, and so on.
     */
    public ResponseData getResponseData(String resumptionToken) 
            throws BadResumptionTokenException,
                   ServerException {
        String[] s = resumptionToken.split("/");
        if (s.length != 2) {
            throw new BadResumptionTokenException(ERR_RESUMPTION_SYNTAX_SLASH);
        }
        int partNum;
        try {
            partNum = Integer.parseInt(s[1]);
        } catch (Exception e) {
            throw new BadResumptionTokenException(ERR_RESUMPTION_SYNTAX_INTEGER);
        }
        Session session;
        synchronized (m_sessions) {
            session = (Session) m_sessions.get(s[0]);
        }
        if (session == null) {
            throw new BadResumptionTokenException(ERR_RESUMPTION_SESSION);
        }
        return session.getResponseData(partNum);
    }

    /////////////////////////////////////////////////////////////////////////

    public void close() {
        m_threadNeedsToFinish = true;
        while (!m_threadFinished) {
            try { Thread.sleep(250); } catch (Exception e) { }
        }
        cleanupSessions(true);
    }

    public void finalize() {
        close();
    }

}