package proai.driver.impl;

import java.util.Iterator;

import proai.driver.RemoteIterator;

public class RemoteIteratorImpl<T> implements RemoteIterator<T> {

    private Iterator<T> m_iter;

    public RemoteIteratorImpl(Iterator<T> iter) {
        m_iter = iter;
    }

    public boolean hasNext() {
        return m_iter.hasNext();
    }

    public T next() {
        return m_iter.next();
    }

    public void close() {
        // do nothing (this impl is based on a non-closable iterator)
    }

    public void remove() {
        throw new UnsupportedOperationException("RemoteIterator does support remove().");
    }

}