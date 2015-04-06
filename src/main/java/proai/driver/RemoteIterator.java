package proai.driver;

import proai.CloseableIterator;
import proai.error.RepositoryException;

public interface RemoteIterator<T> extends CloseableIterator<T> {

    public boolean hasNext() throws RepositoryException;

    public T next() throws RepositoryException;

    public void remove() throws UnsupportedOperationException;

    public void close() throws RepositoryException;
}