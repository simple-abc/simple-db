# Simple DB
______
log: `[InsertLog/UpdateLog[DataItem]]`   
page `[L1Page][Secondary]`  
transaction: `[Super][Normal]`  
## Transaction Manager
______
### Transaction Manager
- builder pattern
- responsible for state of transaction
- maintaining a file starting with a `xid`
## Data Manager
______
### Abstract Cache
- template pattern
### Page

### Page Cache

### Level1 Page, Secondary Page
- format
  - level1 page:
    - when db start up, write random bytes from `OF_VC` to `OF_VC + LEN_VC - 1` byte 
    - when db close, copy random bytes from `OF_VC + LEN_VC` to `OF_VC + 2 * LEN_VC - 1` byte
    - used to determine whether the database was shut down normally in the last time
  - secondary page: `[Offset: 2bytes][Raw]`
    - stored FSO (free space offset)
    - used to process raw data as secondary page when page cache need it in the process of initialization of DataManager
### Logger
- iterator pattern
- format
  - log file: `[Xchecksum][log1][log2][log3]...[logN][BadTail]`
  - log: `[Size][Checksum][Data]`
  - data: 
    - update: `[DataType: 1byte][Xid: 8bytes][Uid: 8bytes][OldRaw][LatestRaw]`
      - uid: `[PageNumber: 4bytes][Offset: 4bytes]`
    - insert: `[DataType: 1byte][Xid: 8bytes][PageNumber: 4bytes][Offset: 2bytes][Raw]`
- process of writing a log
  1. wrap data to log
  2. write log to log file
  3. update check sum
  4. flush to disk
### Page Index
- splitting the secondary page into four-ty block storage
- entry in Map
  - key: the number of free block storage : Integer
  - value: all of PageInfo have the number of free block storage : List
- class 
  - PageInfo
    - pageNumber
    - freeSpace
### Data Item
- format
  - data item: `[ValidFlag: 1byte][DataSize: 3bytes][Raw]`
### Data Manager
- insert()
- read()
