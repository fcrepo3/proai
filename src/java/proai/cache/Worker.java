package proai.cache;

import java.io.*;
import java.util.*;

import net.sf.bvalid.Validator;

import org.apache.log4j.Logger;

import proai.driver.OAIDriver;
import proai.util.StreamUtil;

public class Worker extends Thread {

    private static Logger _LOG = Logger.getLogger(Worker.class.getName());

    private Updater _updater;
    private OAIDriver _driver;
    private RCDisk _disk;
    private Validator _validator;

    private int _attemptedCount;
    private int _failedCount;
    private long _totalFetchTime;
    private long _totalValidationTime;

    public Worker(int num, 
                  int of, 
                  Updater updater, 
                  OAIDriver driver, 
                  RCDisk disk,
                  Validator validator) {
        super("Worker-" + num + "of" + of);
        _updater = updater;
        _driver = driver;
        _disk = disk;
        _validator = validator;
    }

    public void run() {

        _LOG.info("Worker started");

        List<QueueItem> queueItems = _updater.getNextBatch(null);

        while (queueItems != null && !_updater.processingShouldStop()) {

            Iterator<QueueItem> iter = queueItems.iterator();
            while (iter.hasNext() && !_updater.processingShouldStop()) {

                attempt(iter.next());
            }

            if (!_updater.processingShouldStop()) {
                queueItems = _updater.getNextBatch(queueItems);
            } else {
                _LOG.debug("About to finish prematurely because processing should stop");
            }
        }

        _LOG.info("Worker finished");
    }

    private InputStream getRecordStreamForValidation(File recordFile) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        builder.append("<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\">\n");
        builder.append("<responseDate>2002-02-08T08:55:46Z</responseDate>\n");
        builder.append("<request verb=\"GetRecord\" identifier=\"oai:arXiv.org:cs/0112017\" ");
        builder.append("metadataPrefix=\"oai_dc\">http://arXiv.org/oai2</request>\n");
        builder.append("<GetRecord>\n"); 
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(recordFile)));
        String line = reader.readLine();
        while (line != null) {
            builder.append(line + "\n");
            line = reader.readLine();
        }
        builder.append("</GetRecord>\n"); 
        builder.append("</OAI-PMH>"); 
        return new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
    }

    private void attempt(QueueItem qi) {

        RCDiskWriter diskWriter = null;
        long retrievalDelay = 0;
        long validationDelay = 0;
        try {

            diskWriter = _disk.getNewWriter();

            long startFetchTime = System.currentTimeMillis();
            _driver.writeRecordXML(qi.getIdentifier(),
                                   qi.getMDPrefix(),
                                   qi.getSourceInfo(), 
                                   diskWriter);
            diskWriter.flush();
            diskWriter.close();

            long endFetchTime = System.currentTimeMillis();

            retrievalDelay = endFetchTime - startFetchTime;

            if (_validator != null) {
                
                _validator.validate(getRecordStreamForValidation(diskWriter.getFile()),
                                    RecordCache.OAI_RECORD_SCHEMA_URL);
                validationDelay = System.currentTimeMillis() - endFetchTime;
            }
            
            qi.setParsedRecord(new ParsedRecord(qi.getIdentifier(),
                                                qi.getMDPrefix(),
                                                diskWriter.getPath(),
                                                diskWriter.getFile()));

            qi.setSucceeded(true);

            _LOG.info("Successfully processed record");

        } catch (Throwable th) {

            _LOG.warn("Failed to process record", th);

            if (diskWriter != null) {
                diskWriter.close();
                diskWriter.getFile().delete();
            }

            StringWriter failReason = new StringWriter();
            th.printStackTrace(new PrintWriter(failReason, true));
            qi.setFailReason(failReason.toString());
            qi.setFailDate(StreamUtil.nowUTCString());
            _failedCount++;
        } finally {
            _attemptedCount++;
            _totalFetchTime += retrievalDelay;
            _totalValidationTime += validationDelay;
        }
    }

    public int getAttemptedCount() {
        return _attemptedCount;
    }

    public int getFailedCount() {
        return _failedCount;
    }

    public long getTotalFetchTime() {
        return _totalFetchTime;
    }

    public long getTotalValidationTime() {
        return _totalValidationTime;
    }
}
