package proai.service;

import proai.*;
import proai.cache.*;
import proai.error.*;

public class SetListProvider implements ListProvider<SetInfo> {

    private RecordCache m_cache;
    private int m_incompleteListSize;

    public SetListProvider(RecordCache cache,
                           int incompleteListSize) {
        m_cache = cache;
        m_incompleteListSize = incompleteListSize;
    }

    public CloseableIterator<SetInfo> getList() throws ServerException {
        CloseableIterator<SetInfo> iter = m_cache.getSetInfoContent();
        if (iter.hasNext()) return iter;
        try { iter.close(); } catch (Exception e) { }
        throw new NoSetHierarchyException(Responder.ERR_NO_SET_HIERARCHY);
    }

    public CloseableIterator<String[]> getPathList() throws ServerException {
        CloseableIterator<String[]> iter = m_cache.getSetInfoPaths();
        if (iter.hasNext()) return iter;
        try { iter.close(); } catch (Exception e) { }
        throw new NoSetHierarchyException(Responder.ERR_NO_SET_HIERARCHY);
    }

    public RecordCache getRecordCache() {
        return m_cache;
    }

    public int getIncompleteListSize() {
        return m_incompleteListSize;
    }

    public String getVerb() { return "ListSets"; }

}