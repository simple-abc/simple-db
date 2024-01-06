package com.northeastern.edu.simpledb.backend.vm;

import com.northeastern.edu.simpledb.backend.tm.TransactionManager;

public class Visibility {

    /**
     * version skip happens when e has been updated by t1 from v0
     * to v1, but t1 can't be seen by t2, and t2 try to update e,
     * the version of it will transfer from v0 to v2, skipping v1
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level == 0) return false;
        else return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapShot(xmax));
    }

    // determine if record(e) is visible to transaction(t) based on isolation level
    protected static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) return readCommitted(tm, t, e);
        else return repeatableRead(tm, t, e);
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if (xmin == xid && xmax == 0) return true; // e had been created by the current transaction and wasn't deleted

        if (tm.isCommitted(xmin)) {
            if (xmax == 0) return true; // e had been committed and wasn't deleted
            return xmax != xid && !tm.isCommitted(xmax); // e had been deleted by other transaction but wasn't committed
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if (xmin == xid && xmax == 0) return true; // e had been created by the current transaction and wasn't deleted

        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapShot(xmin)) { // e was committed before when the current transaction created
            if (xmax == 0) return true; // not deleted so far
            if (xmax != xid) {
                return !tm.isCommitted(xmax) || xmax > xid || t.isInSnapShot(xmax); // e was deleted by the transaction created after the current transaction
            }
        }
        return false;
    }

}
