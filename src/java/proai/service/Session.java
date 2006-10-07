package proai.service;

import proai.*;
import proai.error.*;

public interface Session {

    /**
     * Has the session expired?
     */
    public boolean hasExpired();

    /**
     * Do all possible cleanup for this session.
     */
    public void clean();

    /**
     * Get the indicated response part (starting with zero).
     */
    public ResponseData getResponseData(int partNum) throws ServerException, 
                                                            BadResumptionTokenException;

}