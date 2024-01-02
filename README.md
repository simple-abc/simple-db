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
### Index Manager
- format
  - Node: `[NumberOfKeys][SiblingUid][Son0][Key0][Son1][Key1]...[SonN][KeyN]`
- insert()
  1. assume `BALANCE` is `3`
      ```mermaid
          graph TD
      A(root) --> B(4)
      A --> C(7)
      A --> D(10)
      A --> E(13)
      B --> F(1)
      B --> G(2)
      B --> H(3)
      C --> I(4)
      C --> J(5)
      C --> K(6)
      D --> L(7)
      D --> M(8)
      D --> N(9)
      E --> O(10)
      E --> P(11)
      E --> Q(12)
      A --> U(MAX)
      U --> R(13)
      U --> S(14)
      U --> T(15)
      U --> V(16)
      U --> W(17)
      ```
  2. after insert `18`, `MAX` will be split, `16` will be created for `13, 14, 15`, then `MAX` will be created for `16, 17, 18`
     ```mermaid
          graph TD
          A(root) --> B(4)
          A --> C(7)
          A --> D(10)
          A --> E(13)
          B --> F(1)
          B --> G(2)
          B --> H(3)
          C --> I(4)
          C --> J(5)
          C --> K(6)
          D --> L(7)
          D --> M(8)
          D --> N(9)
          E --> O(10)
          E --> P(11)
          E --> Q(12)
          A --> U(MAX)
          U --> R(13)
          U --> S(14)
          U --> T(15)
          U --> V(16)
          U --> W(17)
          U --> X(18)
     ```
     ```mermaid
          graph TD
          A(root) --> B(4)
          A --> C(7)
          A --> D(10)
          A --> E(13)
          B --> F(1)
          B --> G(2)
          B --> H(3)
          C --> I(4)
          C --> J(5)
          C --> K(6)
          D --> L(7)
          D --> M(8)
          D --> N(9)
          E --> O(10)
          E --> P(11)
          E --> Q(12)
          A --> Y(16)
          Y --> R(13)
          Y --> S(14)
          Y --> T(15)
          U(MAX) --> V(16)
          U --> W(17)
          U --> X(18)
     ```
  3. `MAX` will be inserted into `root`, so the number of son of `root` exceed limitation
  4. `root` will be split, then another one `MAX` created
     ```mermaid
          graph TD
          A(root) --> B(4)
          A --> C(7)
          A --> D(10)
          B --> F(1)
          B --> G(2)
          B --> H(3)
          C --> I(4)
          C --> J(5)
          C --> K(6)
          D --> L(7)
          D --> M(8)
          D --> N(9)
          E --> O(10)
          E --> P(11)
          E --> Q(12)
          Y --> R(13)
          Y --> S(14)
          Y --> T(15)
          U(MAX) --> V(16)
          U --> W(17)
          U --> X(18)
          Z(MAX) --> E(13)
          Z --> Y(16)
          Z --> U
     ```
  5. create a `newRoot` and replace old root with it
     ```mermaid
     graph TD
     A(13) --> B(4)
     A --> C(7)
     A --> D(10)
     B --> F(1)
     B --> G(2)
     B --> H(3)
     C --> I(4)
     C --> J(5)
     C --> K(6)
     D --> L(7)
     D --> M(8)
     D --> N(9)
     E --> O(10)
     E --> P(11)
     E --> Q(12)
     U --> R(13)
     U --> S(14)
     U --> T(15)
     W --> X(16)
     W --> Y(17)
     W --> Z(18)
     V(Max) --> E(13)
     V --> U(16)
     V --> W(MAX)
     newRoot --> A
     newRoot --> V
     ```
- search()
