package proai.service;

import proai.Writable;
import proai.error.ServerException;

/**
 * The data part of an OAI response.
 * <p/>
 * This may be complete in itself, as in the case of "Identify",
 * or it may be one part in a series of "incomplete list" responses.
 */
public interface ResponseData extends Writable {

    /**
     * Get the resumption token for the next part if this is one
     * in a series of incomplete list responses (and not the last part).
     */
    String getResumptionToken();

    /**
     * Release any resources (such as files or locks on other resources)
     * associated with the response data.
     */
    void release() throws ServerException;

}
