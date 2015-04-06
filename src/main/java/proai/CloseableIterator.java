package proai;

import proai.error.ServerException;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T> {

    public boolean hasNext() throws ServerException;

    public T next() throws ServerException;

    public void remove() throws UnsupportedOperationException;

    public void close() throws ServerException;
    //{
    //    throw new UnsupportedOperationException("CloseableIterator does not support remove().");
    //}

}