package proai;

import java.util.Iterator;

import proai.error.ServerException;

public interface CloseableIterator extends Iterator {

    public boolean hasNext() throws ServerException;

    public Object next() throws ServerException;

    public void close() throws ServerException;

    public void remove() throws UnsupportedOperationException;
    //{
    //    throw new UnsupportedOperationException("CloseableIterator does not support remove().");
    //}

}