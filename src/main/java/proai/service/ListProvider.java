package proai.service;

import proai.CloseableIterator;
import proai.cache.RecordCache;
import proai.error.ServerException;

public interface ListProvider<T> {

    CloseableIterator<T> getList() throws ServerException;

    // abbreviated form of above -- gets String[]s of ("cachePath" [, "dateString"])
    CloseableIterator<String[]> getPathList() throws ServerException;

    RecordCache getRecordCache();

    int getIncompleteListSize();

    String getVerb();

}