package proai.driver;

import proai.CloseableIterator;
import proai.error.RepositoryException;

public interface RemoteIterator<T> extends CloseableIterator<T> {

    boolean hasNext() throws RepositoryException;

    T next() throws RepositoryException;

    void remove() throws UnsupportedOperationException;

    void close() throws RepositoryException;
}