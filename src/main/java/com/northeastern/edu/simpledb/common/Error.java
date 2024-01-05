package com.northeastern.edu.simpledb.common;

public class Error {

    // common
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileExistsException = new RuntimeException("File already exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File cannot read or write!");

    public static final Exception CacheFullException = new RuntimeException("Cache is full!");


    // dm
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");
    public static final Exception BadLogFileException = new RuntimeException("Bad log file!");
    public static Exception DataTooLargeException = new RuntimeException("Data too large!");
    public static Exception DataBaseBusyException = new RuntimeException("Database is too busy!");

    // tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    // vm
    public static Exception NullEntryException = new RuntimeException("Entry is null!");
    public static Exception DeadLockException = new RuntimeException("Deadlock detected!");

    // parser
    public static Exception InvalidCommandException = new RuntimeException("Command is invalid!");
    public static Exception TableNoIndexException = new RuntimeException("Table has no index!");

    // tbm
    public static Exception InvalidFieldException = new RuntimeException("Field is invalid!");
    public static Exception DuplicatedTableException = new RuntimeException("Table already exists!");
    public static Exception TableNotFoundException = new RuntimeException("Table does not exists!");
    public static Exception FieldNotIndexedException = new RuntimeException("Field is not indexed!");
    public static Exception InvalidLogOpException = new RuntimeException("Invalid logic operation!");
}
