package com.northeastern.edu.simpledb.backend.dm.pageIndex;

public class PageInfo {
    public int pageNumber;
    public int freeSpace;

    public PageInfo(int pageNumber, int freeSpace) {
        this.pageNumber = pageNumber;
        this.freeSpace = freeSpace;
    }
}
