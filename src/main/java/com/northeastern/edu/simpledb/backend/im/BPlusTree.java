package com.northeastern.edu.simpledb.backend.im;

import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.DataManger;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.northeastern.edu.simpledb.backend.tm.TransactionManager.SUPER_XID;

public class BPlusTree {

    private static final int UID_SIZE = 8;
    DataManger dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;


    /**
     *  storing uid of insertion of nil root raw, aka rootUid
     *  return the uid of insertion of uid of insertion of nil root raw, aka bootUid
     */
    public static long create(DataManger dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw(); // get nil root raw: [1,0,0,0,0,0,0,0...]
        long rootUid = dm.insert(SUPER_XID, rawRoot); // store nil root raw, and return uid of insertion of nil root raw
        return dm.insert(SUPER_XID, Parser.long2Byte(rootUid)); // store uid, return uid of insertion of uid of nil root raw
    }

    // loading rootUid by bootUid
    public static BPlusTree load(long bootUid, DataManger dm) throws Exception { //
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree bPlusTree = new BPlusTree();
        bPlusTree.bootDataItem = bootDataItem;
        bPlusTree.dm = dm;
        bPlusTree.bootUid = bootUid;
        bPlusTree.bootLock = new ReentrantLock();
        return bPlusTree;
    }

    // retrieving rootUid from the data of bootDataItem
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray subArray = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(subArray.raw, subArray.start, subArray.start + UID_SIZE));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     *      [newRootUid]
     *        /     \
     *   [rootUid] [uid]
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
        long newRootUid = dm.insert(SUPER_XID, rootRaw);
        bootDataItem.before();
        SubArray diRaw = bootDataItem.data();
        System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, UID_SIZE);
        bootDataItem.after(SUPER_XID);
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if (res.newNode != 0) updateRootUid(rootUid, res.newNode, res.newKey);
    }

    class InsertRes {
        long newNode;
        long newKey;
    }

    /**
     * insert uid and key into B+ Tree based on nodeUid
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        // in two situations, insertAndSplit will be invoked
        InsertRes res = null;
        if (isLeaf) {
            // 1. insert to a leaf node
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key); // find the first son has larger key than input key
            InsertRes insertRes = insert(next, uid, key);
            if (insertRes.newNode != 0) {
                // 2. after splitting, a new node created, and this need to be inserted into b+tree based on the current node
                res = insertAndSplit(nodeUid, insertRes.newNode, insertRes.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        for (;;) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes insertAndSplitRes = node.insertAndSplit(uid, key);
            if (insertAndSplitRes.siblingUid != 0) {
                nodeUid = insertAndSplitRes.siblingUid;
            } else {
                InsertRes insertRes = new InsertRes();
                insertRes.newKey = insertAndSplitRes.newKey;
                insertRes.newNode = insertAndSplitRes.newSon;
                return insertRes;
            }
        }
    }

    // search in node starting from root util reaching leaf
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        // System.out.println("node = " + node); for test purpose
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }


    // search uid of key in the current node, if not found return uid of the sibling
    private long searchNext(long nodeUid, long key) throws Exception{
        for(;;) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if (res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    // search() -> searchRange() -> searchLeaf() -> searchNext()
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leftUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        for (;;) { // searching all the siblings starting from the given leaf node and collecting all uids
            Node leaf = Node.loadNode(this, leftUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRangeRes(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if (res.siblingUid == 0) break;
            else leftUid = res.siblingUid;
        }
        return uids;
    }

    public void close() {
        bootDataItem.release();
    }

}
