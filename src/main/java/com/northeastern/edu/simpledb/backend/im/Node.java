package com.northeastern.edu.simpledb.backend.im;

import com.northeastern.edu.simpledb.backend.common.SubArray;
import com.northeastern.edu.simpledb.backend.dm.dataItem.DataItem;
import com.northeastern.edu.simpledb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.northeastern.edu.simpledb.backend.tm.TransactionManager.SUPER_XID;

public class Node {

    // Node will be stored as a DataItem
    // [Valid][Size][LeafFlag][NumberOfKeys][SiblingUid][Son0][Key0][Son1][Key1]...[SonN][KeyN]

    private static final int SON_SIZE = 8;

    private static final int KEY_SIZE = 8;

    private static final int NUMBER_OF_KEYS_SIZE = 2;

    private static final int SIBLING_UID_SIZE = 8;

    private static final int LEAF_FLAG_SIZE = 1;

    private static final int IS_LEAF_OFFSET = 0;

    private static final int NUMBER_OF_KEYS_OFFSET = IS_LEAF_OFFSET + LEAF_FLAG_SIZE;
    private static final int SIBLING_OFFSET = NUMBER_OF_KEYS_OFFSET + NUMBER_OF_KEYS_SIZE;
    private static final int NODE_HEADER_SIZE = SIBLING_OFFSET + SIBLING_UID_SIZE;
    private static final int BALANCE_NUMBER = 3;

    private static final int NODE_SIZE = NODE_HEADER_SIZE + (SON_SIZE + KEY_SIZE) * (BALANCE_NUMBER * 2 + 2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        raw.raw[raw.start + IS_LEAF_OFFSET] = isLeaf ? (byte) 1 : (byte) 0;
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int numberOfKeys) {
        System.arraycopy(Parser.short2Byte((short) numberOfKeys), 0, raw.raw, raw.start + NUMBER_OF_KEYS_OFFSET, NUMBER_OF_KEYS_SIZE);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NUMBER_OF_KEYS_OFFSET, raw.start + NUMBER_OF_KEYS_OFFSET + NUMBER_OF_KEYS_SIZE));
    }

    static void setRawSibling(SubArray raw, long numberOfSiblings) {
        System.arraycopy(Parser.long2Byte(numberOfSiblings), 0, raw.raw, raw.start + SIBLING_OFFSET, SIBLING_UID_SIZE);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + SIBLING_UID_SIZE));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (SON_SIZE + KEY_SIZE);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, SON_SIZE);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (SON_SIZE + KEY_SIZE);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + SON_SIZE));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (SON_SIZE + KEY_SIZE) + SON_SIZE;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, KEY_SIZE);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (SON_SIZE + KEY_SIZE) + SON_SIZE;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + KEY_SIZE));
    }

    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (SON_SIZE + KEY_SIZE);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (SON_SIZE + KEY_SIZE)];
        }
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (SON_SIZE + KEY_SIZE);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem dataItem = bTree.dm.read(uid);
        assert dataItem != null;
        Node node = new Node();
        node.tree = bTree;
        node.dataItem = dataItem;
        node.raw = dataItem.data();
        node.uid = uid;
        return node;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }


    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                if (key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    public LeafSearchRangeRes leafSearchRangeRes(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else break;
            }
            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid;
        long newSon;
        long newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes insertAndSplitRes = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success) {
                insertAndSplitRes.siblingUid = getRawSibling(raw);
                return insertAndSplitRes;
            }
            if (needSplit()) {
                try {
                    SplitRes split = split();
                    insertAndSplitRes.newSon = split.newSon;
                    insertAndSplitRes.newKey = split.newKey;
                    return insertAndSplitRes;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else return insertAndSplitRes;
        } finally {
            if (err == null && success) dataItem.after(SUPER_XID);
            else dataItem.unBefore();
        }
    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        if(getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon;
        long newKey;
    }

    private SplitRes split() throws Exception{
        System.out.println("split...");
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes splitRes = new SplitRes();
        splitRes.newSon = son;
        splitRes.newKey = getRawKthKey(nodeRaw, 0);
        return splitRes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
