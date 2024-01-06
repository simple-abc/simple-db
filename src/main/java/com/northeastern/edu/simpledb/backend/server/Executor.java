package com.northeastern.edu.simpledb.backend.server;

import com.northeastern.edu.simpledb.backend.parser.Parser;
import com.northeastern.edu.simpledb.backend.parser.statement.*;
import com.northeastern.edu.simpledb.backend.tbm.BeginRes;
import com.northeastern.edu.simpledb.backend.tbm.TableManager;
import com.northeastern.edu.simpledb.common.Error;

public class Executor {

    private long xid;
    private TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public byte[] executeTx(byte[] sql) throws Exception {
        System.out.println("sql = " + new String(sql));
        Object statement = Parser.Parse(sql);
        if(statement instanceof Begin) {
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin)statement);
            xid = r.xid;
            return r.result;
        } else if(statement instanceof Commit) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(statement instanceof Abort) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return executeDDLAndDML(statement);
        }
    }

    private byte[] executeDDLAndDML(Object statement) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(statement instanceof Show) {
                res = tbm.show(xid);
            } else if(statement instanceof Create) {
                res = tbm.create(xid, (Create) statement);
            } else if(statement instanceof Select) {
                res = tbm.read(xid, (Select) statement);
            } else if(statement instanceof Insert) {
                res = tbm.insert(xid, (Insert) statement);
            } else if(statement instanceof Delete) {
                res = tbm.delete(xid, (Delete) statement);
            } else if(statement instanceof Update) {
                res = tbm.update(xid, (Update) statement);
            }
            return res;
        } catch(Exception ex) {
            e = ex;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }

    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }
}
