package proai.driver;

import java.io.*;
import java.util.*;

import proai.*;
import proai.error.*;

/**
 * An interface to a repository.
 *
 * Note that an OAIDriver *must* implement a public no-arg constructor,
 * and will be initialized via a call to init(Properties).
 *
 * @author cwilper@cs.cornell.edu
 */
public interface OAIDriver extends Writable {

    /**
     * Initialize from properties.
     *
     * @param props the implementation-specific initialization properties.
     * @throws RepositoryException if required properties are missing/bad, 
     *         or initialization failed for any reason.
     */
    public void init(Properties props) throws RepositoryException;

    /**
     * Write information about the repository to the given PrintWriter.
     * 
     * <p>
     *   This will be a well-formed XML chunk beginning with an 
     *   <code>Identify</code> element, as described in 
     *   <a href="http://www.openarchives.org/OAI/openarchivesprotocol.html#Identify">section
     *   4.2 of the OAI-PMH 2.0 specification</a>.
     * <p>
     *
     * @throws RepositoryException if there is a problem reading from the repository.
     */
    public void write(PrintWriter out) throws RepositoryException;

    /**
     * Get the latest date that something changed in the remote repository.
     *
     * <p>
     *   If this is greater than the previously-aquired latestDate, 
     *   the formats, setInfos, and identity will be retrieved again,
     *   and it will be used as the "until" date for the next record query.
     * </p>
     */
    public Date getLatestDate() throws RepositoryException;

    /**
     * Get an iterator over a list of MetadataFormat objects representing
     * all OAI metadata formats currently supported by the repository.
     *
     * @see proai.MetadataFormat
     */
    public RemoteIterator listMetadataFormats() throws RepositoryException;

    /**
     * Get an iterator over a list of SetInfo objects representing all
     * OAI sets currently supported by the repository.
     *
     * <p>
     *   The content will be a well-formed XML chunk beginning with a
     *   <code>set</code> element, as described in 
     *   <a href="http://www.openarchives.org/OAI/openarchivesprotocol.html#ListSets">section
     *   4.6 of the OAI-PMH 2.0 specification</a>.
     * <p>
     *
     * @see proai.SetInfo
     */
    public RemoteIterator listSetInfo() throws RepositoryException;

    /**
     * Get an iterator of <code>Record</code> objects representing all records 
     * in the format indicated by mdPrefix, which have changed in the given date
     * range.
     *
     * <p><strong>Regarding dates:</strong>
     * <em>If from is not null, the date is greater than (non-inclusive)
     * Until must be specified, and it is less than or equal to (inclusive).</em>
     *
     * @see proai.Record
     */
    public RemoteIterator listRecords(Date from, 
                                      Date until, 
                                      String mdPrefix) throws RepositoryException;

    /**
     * Write the XML of the record whose source info is given.
     *
     * SourceInfo MUST NOT contain newlines. Otherwise, the format is up to the 
     * implementation.
     *
     * The Record implementation produces these strings, and the OAIDriver
     * implementation should know how to use them to produce the XML.
     *
     * The record must be a well-formed XML chunk beginning with a 
     * <code>record</code> element, as described in
     * <a href="http://www.openarchives.org/OAI/openarchivesprotocol.html#GetRecord">section
     * 4.1 of the OAI-PMH 2.0 specification</a>.
     */
    public void writeRecordXML(String itemID,
                               String mdPrefix,
                               String sourceInfo,
                               PrintWriter writer) throws RepositoryException;

    /**
     * Release any resources held by the driver.
     */
    public void close() throws RepositoryException;

}
