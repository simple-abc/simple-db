package com.northeastern.edu.simpledb.backend.dm.page;

public abstract class AbstractPage {

    abstract void lock();

    abstract void unlock();

    abstract void release();

    abstract void setDirty(boolean dirty);

    abstract boolean isDirty();

    abstract int getPageNumber();

    abstract byte[] getData();

}
