package proai.cache;

import net.sf.bvalid.Validator;
import org.apache.log4j.Logger;
import proai.MetadataFormat;
import proai.Record;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.error.ImmediateShutdownException;
import proai.error.RepositoryException;
import proai.error.ServerException;
import proai.util.SetSpec;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class Updater extends Thread {

    private static Logger _LOG = Logger.getLogger(Updater.class.getName());

    private int _pollSeconds;
    private int _maxWorkers;
    private int _maxWorkBatchSize;
    private int _maxFailedRetries;
    private int _maxCommitQueueSize;
    private int _maxRecordsPerTransaction;

    private OAIDriver _driver;
    private RCDatabase _db;
    private RCDisk _disk;
    private Validator _validator;

    private boolean _shutdownRequested;
    private boolean _immediateShutdownRequested;

    private QueueIterator _queueIterator;
    private Worker[] _workers;
    private Committer _committer;
    private boolean _processingAborted;
    private String _status;

    public Updater(OAIDriver driver,
                   RecordCache cache,
                   RCDatabase db,
                   RCDisk disk,
                   int pollSeconds,
                   int maxWorkers,
                   int maxWorkBatchSize,
                   int maxFailedRetries,
                   int maxCommitQueueSize,
                   int maxRecordsPerTransaction,
                   Validator validator) {
        _driver = driver;
        _db = db;
        _disk = disk;

        _pollSeconds = pollSeconds;
        _maxWorkers = maxWorkers;
        _maxWorkBatchSize = maxWorkBatchSize;
        _maxFailedRetries = maxFailedRetries;
        _maxCommitQueueSize = maxCommitQueueSize;
        _maxRecordsPerTransaction = maxRecordsPerTransaction;
        _validator = validator;
    }

    public void run() {

        _status = "Started";
        while (!_shutdownRequested) {

            long cycleStartTime = System.currentTimeMillis();

            _LOG.info("Update cycle initiated");

            try {


                // It's important to do this first because old items may have
                // been left in the queue due to an immediate or improper
                // shutdown.  This ensures that unintentional duplicates 
                // (especially old failures) aren't entered into the queue 
                // during the polling+updating phase.
                _status = "Processing any old items in queue";
                checkImmediateShutdown();
                processQueue("old");

                checkImmediateShutdown();
                _status = "Polling and updating queue and database";
                pollAndUpdate();

                _status = "Processing any new items in queue";
                checkImmediateShutdown();
                processQueue("new");

                checkImmediateShutdown();
                _status = "Pruning old files from cache if needed";
                pruneIfNeeded();

                long sec = (System.currentTimeMillis() - cycleStartTime) / 1000;
                _LOG.info("Update cycle finished in " + sec + "sec."
                        + "Next cycle scheduled in " + _pollSeconds + "sec.");

            } catch (ImmediateShutdownException e) {
                _LOG.info("Update cycle aborted due to immediate shutdown request");
            } catch (Throwable th) {
                _LOG.error("Update cycle failed", th);
            }

            _status = "Sleeping";
            int waitedSeconds = 0;
            while (!_shutdownRequested && waitedSeconds < _pollSeconds) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
                waitedSeconds++;
            }

        }
        _status = "Finished";

    }

    private void checkImmediateShutdown() throws ImmediateShutdownException {
        if (_immediateShutdownRequested) {
            throw new ImmediateShutdownException();
        }
    }

    /**
     * Process the queue till it's empty.
     */
    private void processQueue(String kind) throws Exception {

        _LOG.info("Processing " + kind + " records in queue...");

        int itemsInQueue = countItemsInQueue();

        checkImmediateShutdown();
        if (itemsInQueue > 0) {

            long processingStartTime = System.currentTimeMillis();
            _processingAborted = false;

            while (itemsInQueue > 0 && !_processingAborted) {

                try {

                    _queueIterator = newQueueIterator();

                    // the committer must exist before the workers are started
                    _committer = new Committer(this,
                            _db,
                            _maxCommitQueueSize,
                            _maxRecordsPerTransaction);

                    // decide how many workers to create (1 to _maxWorkers)
                    int numWorkers = itemsInQueue / _maxWorkBatchSize;
                    if (numWorkers > _maxWorkers) numWorkers = _maxWorkers;
                    if (numWorkers == 0) numWorkers = 1;

                    _LOG.info("Queue has " + itemsInQueue + " records.  Starting "
                            + numWorkers + " worker threads for processing.");

                    // start the workers
                    _workers = new Worker[numWorkers];
                    for (int i = 0; i < _workers.length; i++) {
                        _workers[i] = new Worker(i + 1,
                                _workers.length,
                                this,
                                _driver,
                                _disk,
                                _validator);
                        _workers[i].start();
                    }

                    // the workers must exist before the committer is started
                    _committer.start();

                    // wait for workers and committer to finish
                    while (_committer.isAlive()) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                        }
                    }

                    checkImmediateShutdown();


                } finally {

                    // clean up and log stats for this round of processing
                    if (_queueIterator != null) {
                        _queueIterator.close();
                    }

                    if (_workers != null) {
                        logProcessingStats(itemsInQueue,
                                System.currentTimeMillis() - processingStartTime);
                        _workers = null;
                        _committer = null;
                    }
                }

                itemsInQueue = countItemsInQueue();
            }

            if (_processingAborted) {
                throw new ServerException("Queue processing was aborted due to unexpected error (see above)");
            }

        } else {
            _LOG.info("Queue is empty.  No processing needed.");
        }

    }

    private void pollAndUpdate() throws ServerException {

        Connection conn = null;
        boolean startedTransaction = false;
        try {
            conn = RecordCache.getConnection();
            conn.setAutoCommit(false);
            startedTransaction = true;

            _db.queueFailedRecords(conn, _maxFailedRetries);

            if (_db.isPollingEnabled(conn)) {
                long latestRemoteDate = _driver.getLatestDate().getTime();
                if (latestRemoteDate > _db.getEarliestPollDate(conn)) {

                    _LOG.info("Starting update process; source data of interest may have changed.");
                    checkImmediateShutdown();
                    updateIdentify(conn);

                    checkImmediateShutdown();
                    List<String> allPrefixes = updateFormats(conn);

                    checkImmediateShutdown();
                    updateSets(conn);

                    checkImmediateShutdown();
                    queueUpdatedRecords(conn, allPrefixes, latestRemoteDate);
                } else {
                    _LOG.info("Skipping update process; source data of interest has not changed");
                }
            } else {
                _LOG.info("Remote polling skipped -- polling is disabled");
            }

            conn.commit();
        } catch (Throwable th) {
            if (startedTransaction) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    _LOG.error("Failed to roll back failed transaction", e);
                }
            }
            throw new ServerException("Update cycle phase one aborted", th);
        } finally {
            if (conn != null) {
                try {
                    if (startedTransaction) conn.setAutoCommit(false);
                } catch (SQLException e) {
                    _LOG.error("Failed to set autoCommit to false", e);
                } finally {
                    RecordCache.releaseConnection(conn);
                }
            }
        }

    }

    private void pruneIfNeeded() throws Exception {

        Connection conn = null;
        File resultFile = null;
        PrintWriter resultWriter = null;
        BufferedReader resultReader = null;

        try {
            conn = RecordCache.getConnection();

            if (_db.getPrunableCount(conn) > 0) {

                resultFile = File.createTempFile("proai-prunable", ".txt");
                resultWriter = new PrintWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(resultFile), "UTF-8"));

                int numToPrune = _db.dumpPrunables(conn, resultWriter);
                resultWriter.close();

                _LOG.info("Pruning " + numToPrune + " old files from cache");
                resultReader = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(resultFile), "UTF-8"));

                int i = 0;
                int[] toPruneKeys = new int[32];

                String line = resultReader.readLine();

                while (line != null) {

                    String[] parts = line.split(" ");
                    if (parts.length == 2) {

                        int pruneKey = Integer.parseInt(parts[0]);
                        File file = _disk.getFile(parts[1]);

                        if (file.exists()) {
                            boolean deleted = file.delete();
                            if (deleted) {
                                _LOG.debug("Deleted old cache file: " + parts[1]);
                            } else {
                                _LOG.warn("Unable to delete old cache file (will try again later): " + parts[1]);
                            }
                        } else {
                            _LOG.debug("No need to delete non-existing old cache file: " + parts[1]);
                        }

                        // delete from prune list if it no longer exists
                        toPruneKeys[i++] = pruneKey;
                        if (i == toPruneKeys.length) {
                            _db.deletePrunables(conn, toPruneKeys, i);
                            i = 0;
                        }
                    }

                    line = resultReader.readLine();
                }

                // do final chunk if needed
                if (i > 0) {
                    _db.deletePrunables(conn, toPruneKeys, i);
                }
            } else {
                _LOG.info("Pruning is not needed.");
            }

        } finally {
            if (resultWriter != null) {
                try {
                    resultWriter.close();
                } catch (Exception e) {
                }
                if (resultReader != null) {
                    try {
                        resultReader.close();
                    } catch (Exception e) {
                    }
                }
            }
            if (resultFile != null) {
                resultFile.delete();
            }
            RecordCache.releaseConnection(conn);
        }

    }

    private int countItemsInQueue() throws Exception {
        Connection conn = RecordCache.getConnection();
        try {
            return _db.getQueueSize(conn);
        } finally {
            RecordCache.releaseConnection(conn);
        }
    }

    /**
     * Get a new <code>QueueIterator</code> over the current queue.
     */
    private QueueIterator newQueueIterator() throws Exception {

        Connection conn = null;
        File queueFile = null;
        PrintWriter queueWriter = null;
        try {

            conn = RecordCache.getConnection();

            queueFile = File.createTempFile("proai-queue", ".txt");
            queueWriter = new PrintWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(queueFile), "UTF-8"));
            _db.dumpQueue(conn, queueWriter);
            queueWriter.close();

            return new QueueIterator(queueFile);

        } finally {
            if (queueWriter != null) {
                try {
                    queueWriter.close();
                } catch (Exception e) {
                }
            }
            if (queueFile != null) {
                queueFile.delete();
            }
            RecordCache.releaseConnection(conn);
        }

    }

    /**
     * Log stats for a round of processing.
     * <p/>
     * This assumes the array of workers and the committer have been
     * initialized.
     */
    private void logProcessingStats(int initialQueueSize,
                                    long totalDuration) {

        StringBuffer stats = new StringBuffer();

        int recordsProcessed = _committer.getProcessedCount();

        stats.append("    Records processed        : " + recordsProcessed + " of " + initialQueueSize + " on queue\n");
        stats.append("    Total processing time    : " + getHMSString(totalDuration) + "\n");

        double processingRate = (double) recordsProcessed / ((double) totalDuration / 1000.0);
        stats.append("    Processing rate          : " + round(processingRate) + " records/second\n");
        stats.append("    Workers spawned          : " + _workers.length + " of " + _maxWorkers + " maximum\n");

        int failedCount = 0;
        int attemptedCount = 0;
        long totalFetchTime = 0;
        for (int i = 0; i < _workers.length; i++) {
            failedCount += _workers[i].getFailedCount();
            attemptedCount += _workers[i].getAttemptedCount();
            totalFetchTime += _workers[i].getTotalFetchTime();
        }
        stats.append("    Failed record loads      : " + failedCount + " of " + attemptedCount + " attempted\n");
        long msPerAttempt = totalFetchTime / attemptedCount;
        stats.append("    Avg roundtrip fetch time : " + getHMSString(msPerAttempt) + "\n");

        int transactionCount = _committer.getTransactionCount();
        stats.append("    Total DB transactions    : " + transactionCount + "\n");

        stats.append("    Total transaction time   : " + getHMSString(_committer.getTotalCommitTime()) + "\n");
        long msPerTrans = Math.round((double) _committer.getTotalCommitTime() / (double) transactionCount);
        stats.append("    Avg time/transaction     : " + getHMSString(msPerTrans) + "\n");

        double recsPerTrans = (double) recordsProcessed / (double) transactionCount;
        stats.append("    Avg recs/transaction     : " + round(recsPerTrans) + " of " + _maxRecordsPerTransaction + " maximum\n");

        _LOG.info("A round of queue processing has finished.\n\nProcessing Stats:\n"
                + stats.toString());

    }

    private void updateIdentify(Connection conn) throws Exception {

        _LOG.info("Getting 'Identify' xml from remote source...");

        _db.setIdentifyPath(conn, _disk.write(_driver));
    }

    /**
     * Update all formats and return the latest list of mdPrefixes.
     * <p/>
     * <p>This will add any new formats, modify any changed formats,
     * and delete any no-longer-existing formats (and associated records).
     */
    private List<String> updateFormats(Connection conn) throws Exception {

        _LOG.info("Updating metadata formats...");

        // apply new / updated
        RemoteIterator<? extends MetadataFormat> riter = _driver.listMetadataFormats();
        List<String> newPrefixes = new ArrayList<String>();
        try {
            while (riter.hasNext()) {

                checkImmediateShutdown();
                MetadataFormat format = (MetadataFormat) riter.next();
                _db.putFormat(conn, format);
                newPrefixes.add(format.getPrefix());
            }
        } finally {
            try {
                riter.close();
            } catch (Exception e) {
                _LOG.warn("Unable to close remote metadata format iterator", e);
            }
        }

        // apply deleted
        Iterator<CachedMetadataFormat> iter = _db.getFormats(conn).iterator();
        while (iter.hasNext()) {

            CachedMetadataFormat format = iter.next();
            String oldPrefix = format.getPrefix();
            if (!newPrefixes.contains(oldPrefix)) {

                checkImmediateShutdown();
                _db.deleteFormat(conn, oldPrefix);
            }
        }

        return newPrefixes;
    }

    /**
     * Update all sets.
     * <p/>
     * <p>This will add any new sets, modify any changed sets, and delete any
     * no-longer-existing sets (and associated membership data).
     */
    private void updateSets(Connection conn) throws Exception {

        _LOG.info("Updating sets...");

        // apply new / updated
        RemoteIterator<? extends SetInfo> riter = _driver.listSetInfo();
        Set<String> newSpecs = new HashSet<String>();
        Set<String> missingSpecs = new HashSet<String>();

        try {
            while (riter.hasNext()) {

                checkImmediateShutdown();
                SetInfo setInfo = (SetInfo) riter.next();
                String encounteredSetSpec = setInfo.getSetSpec();

                /*
                 * If we encounter a setSpec that implies that it is
                 * a subset, look for the parent.  If we haven't
                 * encountered its parent yet, remember its identity:
                 * unless we encounter it in subsequent results, we'll
                 * have to use a default placeholder for it later.
                 */
                if (SetSpec.hasParents(encounteredSetSpec) &&
                        !newSpecs.contains(
                                SetSpec.parentOf(encounteredSetSpec))) {
                    missingSpecs.add(SetSpec.parentOf(encounteredSetSpec));
                }
                _db.putSetInfo(
                        conn, encounteredSetSpec, _disk.write(setInfo));
                newSpecs.add(encounteredSetSpec);
            }
        } finally {
            try {
                riter.close();
            } catch (Exception e) {
                _LOG.warn("Unable to close remote set info iterator", e);
            }
        }

        /* Add any sets that are IMPLIED to exist, but weren't defined */
        for (String possiblyMissing : missingSpecs) {

            if (!SetSpec.isValid(possiblyMissing)) {
                throw new RepositoryException("SetSpec '" + possiblyMissing
                        + "' is malformed");
            }

            for (String spec : SetSpec.allSetsFor(possiblyMissing)) {
                if (!newSpecs.contains(spec)) {
                    _db.putSetInfo(conn, spec, _disk.write(
                            SetSpec.defaultInfoFor(spec)));
                    newSpecs.add(spec);
                    _LOG.warn("Adding missing set: " + spec);
                }
            }
        }

        // apply deleted
        Iterator<SetInfo> iter = _db.getSetInfo(conn).iterator();
        while (iter.hasNext()) {

            String oldSpec = ((SetInfo) iter.next()).getSetSpec();
            if (!newSpecs.contains(oldSpec)) {

                checkImmediateShutdown();
                _db.deleteSet(conn, oldSpec);
            }
        }
    }

    private void queueUpdatedRecords(Connection conn,
                                     List<String> allPrefixes,
                                     long latestRemoteDate) throws Exception {

        _LOG.info("Querying and queueing updated records...");

        long queueStartTime = System.currentTimeMillis();
        int totalQueuedCount = 0;
        ;
        for (String mdPrefix : allPrefixes) {

            long lastPollDate = _db.getLastPollDate(conn, mdPrefix);

            // if something may have changed remotely *after* the last
            // known date that any records of this format were queried for,
            // query for updated records
            if (lastPollDate < latestRemoteDate) {

                _LOG.info("Querying for changed " + mdPrefix + " records because "
                        + lastPollDate + " is less than " + latestRemoteDate);

                checkImmediateShutdown();
                RemoteIterator<? extends Record> riter = _driver.listRecords(new Date(lastPollDate),
                        new Date(latestRemoteDate),
                        mdPrefix);
                try {

                    int queuedCount = 0;

                    while (riter.hasNext()) {

                        Record record = riter.next();
                        checkImmediateShutdown();
                        _db.queueRemoteRecord(conn,
                                record.getItemID(),
                                record.getPrefix(),
                                record.getSourceInfo());
                        queuedCount++;
                    }

                    _LOG.info("Queued " + queuedCount + " new/modified "
                            + mdPrefix + " records.");

                    _db.setLastPollDate(conn, mdPrefix, latestRemoteDate);

                    totalQueuedCount += queuedCount;
                } finally {
                    try {
                        riter.close();
                    } catch (Exception e) {
                        _LOG.warn("Unable to close remote record iterator", e);
                    }
                }
            } else {
                _LOG.info("Skipping " + mdPrefix + " records because "
                        + lastPollDate + " is not less than "
                        + latestRemoteDate);
            }
        }

        long sec = (System.currentTimeMillis() - queueStartTime) / 1000;
        _LOG.info("Queued " + totalQueuedCount
                + " total new/modified records in " + sec + "sec.");
    }

    /**
     * For the given number of milliseconds, return a string like this:
     * <p/>
     * <p><pre>[h hours, ][m minutes, ]sec.ms seconds</pre>
     */
    private static String getHMSString(long ms) {

        StringBuffer out = new StringBuffer();

        long hours = ms / (1000 * 60 * 60);
        ms -= hours * 1000 * 60 * 60;
        long minutes = ms / (1000 * 60);
        ms -= minutes * 1000 * 60;
        long seconds = ms / 1000;
        ms -= seconds * 1000;

        if (hours > 0) {
            out.append(hours + " hours, ");
        }
        if (minutes > 0) {
            out.append(minutes + " minutes, ");
        }

        String msString;
        if (ms > 99) {
            msString = "." + ms;
        } else if (ms > 9) {
            msString = ".0" + ms;
        } else if (ms > 0) {
            msString = ".00" + ms;
        } else {
            msString = ".000";
        }

        out.append(seconds + msString + " seconds");

        return out.toString();
    }

    private static double round(double val) {
        return (double) Math.round(val * 100.0) / 100.0;
    }

    /**
     * Signal that the thread should be shut down and wait for it to finish.
     * <p/>
     * If immediate is true, abort the update cycle if it's running.
     */
    public void shutdown(boolean immediate) {

        if (this.isAlive()) {

            _shutdownRequested = true;
            _immediateShutdownRequested = immediate;

            while (this.isAlive()) {
                _LOG.info("Waiting for updater to finish.  Current status: " + _status);
                try {
                    Thread.sleep(250);
                } catch (Exception e) {
                }
            }

            _LOG.info("Updater shutdown complete");
        }
    }

    /**
     * Handle an exception encountered by currently-running Committer while
     * committing.
     */
    protected void handleCommitException(Throwable th) {
        _LOG.warn("Processing aborted due to commit failure", th);
        _processingAborted = true;
    }

    // return null if no more batches or processing should stop
    protected List<QueueItem> getNextBatch(List<QueueItem> finishedItems) {

        List<QueueItem> nextBatch = null;

        if (!processingShouldStop()) {

            if (finishedItems != null) {
                _committer.handoff(finishedItems);
            }

            try {
                synchronized (_queueIterator) {
                    if (_queueIterator.hasNext()) {
                        nextBatch = new ArrayList<QueueItem>();
                        while (_queueIterator.hasNext() &&
                                nextBatch.size() < _maxWorkBatchSize) {
                            nextBatch.add(_queueIterator.next());
                        }
                    }
                }
            } catch (Throwable th) {
                _LOG.warn("Processing aborted due to commit failure", th);
                synchronized (this) {
                    _processingAborted = true;
                }
                nextBatch = null;
            }

        }

        return nextBatch;
    }

    protected synchronized boolean processingShouldStop() {
        return _processingAborted || _immediateShutdownRequested;
    }

    protected boolean anyWorkersAreRunning() {
        if (_workers == null) {
            return false;
        } else {
            for (int i = 0; i < _workers.length; i++) {
                if (_workers[i].isAlive()) return true;
            }
            return false;
        }
    }
}
