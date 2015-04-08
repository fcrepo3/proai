package proai;

import proai.error.ServerException;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T> {

    boolean hasNext() throws ServerException;

    T next() throws ServerException;

    void remove() throws UnsupportedOperationException;

    void close() throws ServerException;
    //{
    //    throw new UnsupportedOperationException("CloseableIterator does not support remove().");
    //}

}