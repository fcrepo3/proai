package proai.service;

import proai.*;
import proai.cache.*;
import proai.error.*;

public interface ListProvider {

    public CloseableIterator getList() throws ServerException;

    // abbreviated form of above -- gets String[]s of ("cachePath" [, "dateString"])
    public CloseableIterator getPathList() throws ServerException;

    public RecordCache getRecordCache();

    public int getIncompleteListSize();

    public String getVerb();

}