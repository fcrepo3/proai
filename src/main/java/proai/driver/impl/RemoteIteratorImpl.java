package proai.driver.impl;

import proai.driver.RemoteIterator;

import java.util.Iterator;

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

    public void remove() {
        throw new UnsupportedOperationException("RemoteIterator does support remove().");
    }

    public void close() {
        // do nothing (this impl is based on a non-closable iterator)
    }

}