package proai.service;

import proai.error.BadResumptionTokenException;
import proai.error.ServerException;

public interface Session {

    /**
     * Has the session expired?
     */
    boolean hasExpired();

    /**
     * Do all possible cleanup for this session.
     */
    void clean();

    /**
     * Get the indicated response part (starting with zero).
     */
    ResponseData getResponseData(int partNum) throws ServerException;

}