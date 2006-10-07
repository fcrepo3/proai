package proai.driver;

import proai.CloseableIterator;
import proai.error.RepositoryException;

public interface RemoteIterator extends CloseableIterator {

    public boolean hasNext() throws RepositoryException;

    public Object next() throws RepositoryException;

    public void close() throws RepositoryException;

    public void remove() throws UnsupportedOperationException;
}