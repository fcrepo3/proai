package proai.cache;

import java.util.*;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import proai.error.ServerException;
import proai.util.StreamUtil;

/**
 * A thread for committing a series of <code>QueueItem</code> updates to the
 * database.
 *
 * <p>Items are added to a "commit queue" by <code>Worker</code> threads via the 
 * synchronized handoff() method.  This can occur before the thread is actually
 * started.  Once the thread is started, the items are removed asynchronously, 
 * then committed, until all workers have finished and ( the queue is empty
 * or processing has been aborted ).
 *
 * @author Chris Wilper
 */ 
public class Committer extends Thread {

    private static Logger _LOG = Logger.getLogger(Committer.class.getName());

    private Updater _updater;
    private RCDatabase _db;
    private int _maxCommitQueueSize;
    private int _maxRecordsPerTransaction;

    private Map _formatKeyMap;

    private List _commitQueue;
    private int _lastCommitQueueSize;

    /**
     * This lock is used to ensure threadsafe access to the _lastCommitQueueSize 
     * primitive.  By design, only two threads will ever be contending for it.
     */
    private Object _lastCommitQueueSizeLock = new Object();

    /**
     * Only true if the thread has been started and has finished.
     */
    private boolean _finishedRunning;

    private int _transactionCount;
    private int _processedCount;
    private long _totalCommitTime;

    /**
     * Construct a new committer with the given configuration.
     *
     * The caller is responsible for actually starting the thread.
     */
    public Committer(Updater updater, 
                     RCDatabase db,
                     int maxCommitQueueSize,
                     int maxRecordsPerTransaction) throws ServerException {

        super("Committer");
        _updater = updater;
        _db = db;
        _maxCommitQueueSize = maxCommitQueueSize;
        _maxRecordsPerTransaction = maxRecordsPerTransaction;

        _commitQueue = new ArrayList(_maxCommitQueueSize);

        // get this now -- it won't change while the thread is running
        Connection conn = null;
        try {
            conn = RecordCache.getConnection();
            _formatKeyMap = _db.getFormatKeyMap(conn);
        } catch (SQLException e) {
            throw new ServerException("Error getting connection while "
                                    + "initializing Committer", e);
        } finally {
            RecordCache.releaseConnection(conn);
        }
    }

    /**
     * Attempt to add the list of <code>QueueItems</code> to the commit queue
     * and return immediately.
     *
     * This method will block until adding the list would not cause the queue
     * to exceed its capacity or the thread is finished running.
     *
     * @return whether the handoff was successful.  The handoff will only
     *         fail if the <code>Committer</code> thread has been stopped.
     */
    protected synchronized boolean handoff(List queueItems) {

        int toAddSize = queueItems.size();
        while (!_finishedRunning && 
               (getLastCommitQueueSize() + toAddSize) > _maxCommitQueueSize) {
            _LOG.debug("Commit queue is too big; waiting for it shrink "
                    + "or for Committer thread to finish");
            try { Thread.sleep(100); } catch (Exception e) { }
        }
        if (!_finishedRunning) {
            synchronized (_commitQueue) {
                _commitQueue.addAll(queueItems);
                setLastCommitQueueSize(_commitQueue.size());
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Run the thread.
     *
     * This works in two phases.  The first phase executes while workers are
     * running and the second phase commits all remaining items unless
     * processing is aborted.
     */
    public void run() {

        _LOG.info("Committer started");

        // phase one
        while (_updater.anyWorkersAreRunning()) {
            List nextItems = getNextTransactionItems();
            while (nextItems == null && _updater.anyWorkersAreRunning()) {
                // wait for the queue to have items
                _LOG.debug("Commit queue is empty; waiting for worker(s)");
                try { Thread.sleep(100); } catch (Exception e) { }
                nextItems = getNextTransactionItems();
            }
            if (nextItems != null) {
                commit(nextItems);
            }
        }

        // phase two
        List lastItems = getNextTransactionItems();
        while (!_updater.processingShouldStop() && lastItems != null) {
            commit(lastItems);
            lastItems = getNextTransactionItems();
        }

        _LOG.info("Committer finished");
        _finishedRunning = true;

    }

    private void commit(List items) {

        Connection conn = null;
        boolean startedTransaction = false;

        long commitStartTime = System.currentTimeMillis();
        try {

            conn = RecordCache.getConnection();
            conn.setAutoCommit(false);
            startedTransaction = true;

            // update the database for each record, as necessary
            Iterator iter = items.iterator();
            while (iter.hasNext()) {
                updateItem(conn, (QueueItem) iter.next());
            }

            // set the estimated commit date for all added/modified records
            // in the transaction.  This obviously can't be exact, so we put 
            // it into the future by a few seconds to be on the safe side.
            Date cacheCommitDate = new Date(StreamUtil.nowUTC().getTime() + 5000);

            _db.setUncommittedRecordDates(conn, cacheCommitDate);

            // finally, commit
            conn.commit();
            _transactionCount++;
            _processedCount += items.size();
            _totalCommitTime += System.currentTimeMillis() - commitStartTime;
            _LOG.info("Committed " + items.size() + " QueueItems to database");
            
            // before returning, check if our cacheCommitDate estimate was ok
            Date now = StreamUtil.nowUTC();
            if (cacheCommitDate.getTime() < now.getTime()) {
                long diff = now.getTime() - cacheCommitDate.getTime();
                _LOG.warn("Commit took longer than expected.  cacheCommitDate "
                        + "estimate was therefore not safe.  If any "
                        + "harvest requests specifying until=null started "
                        + "within the last " + diff + "ms., they might "
                        + "have missed these records.");
            }

        } catch (Throwable th) {
            // roll back uncommitted db updates
            if (startedTransaction) {
                try {
                    conn.rollback();
                } catch (Exception e) {
                    _LOG.error("Failed to roll back failed transaction", e);
                }
            }

            // ...delete uncommitted files
            Iterator toDelete = items.iterator();
            while (toDelete.hasNext()) {
                ParsedRecord pr = ((QueueItem) toDelete.next()).getParsedRecord();
                if (pr != null) pr.deleteFile();
            }

            // ...then signal error to updater
            _updater.handleCommitException(th);

        } finally {
            // release the connection in its original state
            if (conn != null) {
                try {
                    if (startedTransaction) conn.setAutoCommit(false);
                } catch (Exception e) {
                    _LOG.error("Failed to set autoCommit to false", e);
                } finally {
                    RecordCache.releaseConnection(conn);
                }
            }
        }
    }

    private void updateItem(Connection conn,
                            QueueItem item) throws Exception {

        _db.removeFromQueue(conn, item.getQueueKey());
        if (item.succeeded()) {
            _db.putRecord(conn, item.getParsedRecord(), _formatKeyMap);
            if (item.getQueueSource() == 'F') {
                _db.removeFailure(conn, item.getIdentifier(), item.getMDPrefix());
            }
        } else {
            int oldFailCount = _db.getFailCount(conn,
                                                item.getIdentifier(), 
                                                item.getMDPrefix());

            if (oldFailCount == -1) {
                _db.addFailure(conn, 
                               item.getIdentifier(), 
                               item.getMDPrefix(),
                               item.getSourceInfo(),
                               item.getFailDate(), 
                               item.getFailReason());
            } else {
                _db.updateFailure(conn, 
                                  item.getIdentifier(), 
                                  item.getMDPrefix(),
                                  item.getSourceInfo(),
                                  oldFailCount + 1,
                                  item.getFailDate(), 
                                  item.getFailReason());
            }
        }
    }

    /**
     * If any items are currently on the commit queue, take up to 
     * the maximum per-transaction off the queue and return them.
     *
     * Otherwise, return <code>null</code>.
     */
    private List getNextTransactionItems() {

        synchronized (_commitQueue) {
            if (_commitQueue.size() == 0) {
                return null;
            } else {
                List nextItems = new ArrayList();
                while ( (_commitQueue.size() > 0) &&
                        (nextItems.size() < _maxRecordsPerTransaction) ) {
                    nextItems.add(_commitQueue.remove(0));
                }
                setLastCommitQueueSize(_commitQueue.size());
                return nextItems;
            }
        }
        
    }


    /**
     * Get the last known queue size as long as the last known size is not 
     * currently being written.  The caller does not need to have a lock on
     * the entire queue.
     */
    private int getLastCommitQueueSize() {
        synchronized (_lastCommitQueueSizeLock) {
            return _lastCommitQueueSize;
        }
    }

    /**
     * Set the last known queue size as long as the last known size is not
     * currently being read. The caller should already have a lock on the queue.
     */
    private void setLastCommitQueueSize(int size) {
        synchronized (_lastCommitQueueSizeLock) {
            _lastCommitQueueSize = size;
        }
    }

    protected int getTransactionCount() {
        return _transactionCount;
    }

    protected int getProcessedCount() {
        return _processedCount;
    }

    protected long getTotalCommitTime() {
        return _totalCommitTime;
    }
}