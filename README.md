<img src="logo/bustub-whiteborder.svg" alt="BusTub Logo" height="200">

---

[![Build Status](https://github.com/cmu-db/bustub/actions/workflows/cmake.yml/badge.svg)](https://github.com/cmu-db/bustub/actions/workflows/cmake.yml)


<img src="logo/sql.png" alt="BusTub SQL" width="400">

BusTub is a relational database management system built at at [Carnegie Mellon University](https://db.cs.cmu.edu) for the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course. It implements core database functionality including buffer pool management, indexing, query execution, and concurrency control. The system is designed to be thread-safe and supports concurrent operations while maintaining data consistency. BusTub supports basic SQL and comes with an interactive shell. Below are the key components that I have implemented:

### Project 1: Buffer Pool Manager

[Project Specification](https://15445.courses.cs.cmu.edu/spring2025/project1/)

- **LRU-K Replacement Policy**

  - Thread-safe LRU-K page replacement algorithm
  - Tracks page usage history with k-distance
  - Evicts pages based on backward k-distance
  - Maintains access history for each frame
  - Supports frame pinning and unpinning

- **Disk Scheduler**

  - Asynchronous I/O operations management
  - Background worker thread for request processing
  - Thread-safe request queue implementation
  - Efficient disk request scheduling
  - Support for read and write operations

- **Buffer Pool Manager**
  - Thread-safe page caching system
  - Page allocation and deallocation
  - Page eviction with LRU-K policy
  - Disk I/O operations coordination
  - Frame management and pinning

### Project 2: B+Tree Index

[Project Specification](https://15445.courses.cs.cmu.edu/spring2025/project2/)

- **B+Tree Index**
  - Thread-safe B+Tree implementation
  - Support for:
    - Insert, delete, and lookup operations
    - Range scans with iterator interface
    - Concurrent operations with proper locking
    - Leaf node merging and redistribution
    - Internal node splitting and merging
  - Efficient key-value storage
  - Support for variable-length keys
  - Proper page management and buffer pool integration

### Project 3: Query Execution

[Project Specification](https://15445.courses.cs.cmu.edu/spring2025/project3/)

- **Query Execution Engine**
  - **Sequential Scan**
    - Table heap traversal
    - Predicate filtering
    - Projection support
  - **Index Scan**
    - B+Tree index traversal
    - Range scan support
    - Key-based lookups
  - **Join Executors**
    - Nested Loop Join
    - Hash Join
  - **Aggregation**
    - Group by operations
    - Aggregate functions
  - **Sort**
    - External merge sort
    - Top-N optimization
  - **Projection**
    - Column selection
    - Expression evaluation
  - **Filter**
    - Predicate evaluation
    - Boolean expression support

### Project 4: Concurrency Control

[Project Specification](https://15445.courses.cs.cmu.edu/spring2025/project4/)

- **Transaction Management**

  - Timestamp allocation for read and commit operations
  - MVCC (Multi-Version Concurrency Control) protocol
  - Snapshot isolation level implementation
  - Transaction state tracking and management
  - Undo log management
  - Garbage collection support

- **Storage Format**

  - Version chains for tuple history
  - Undo logs for transaction rollback
  - Tuple metadata management
  - Version visibility tracking
  - Efficient storage layout

- **MVCC Executors**

  - **Sequential Scan**
    - Version visibility checking
    - Snapshot isolation support
  - **Index Scan**
    - Version-aware lookups
    - Concurrent access handling
  - **Modification Executors**
    - Insert
    - Update
    - Delete
    - Version chain management
    - Undo log generation

- **Primary Key Index**
  - Unique constraint enforcement
  - Version-aware lookups
  - Concurrent access handling
  - Index maintenance during updates
  - Proper version chain integration

## Build

We recommend developing BusTub on Ubuntu 22.04, or macOS (M1/M2/Intel). We do not support any other environments. We do not support WSL.

### Linux (Recommended) / macOS (Development Only)

To ensure that you have the proper packages on your machine, run the following script to automatically install them:

```console
# Linux
$ sudo build_support/packages.sh
# macOS
$ build_support/packages.sh
```

Then run the following commands to build the system:

```console
$ mkdir build
$ cd build
$ cmake ..
$ make
```

If you want to compile the system in debug mode, pass in the following flag to cmake:
Debug mode:

```console
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make -j`nproc`
```

This enables [AddressSanitizer](https://github.com/google/sanitizers) by default.

If you want to use other sanitizers,

```console
$ cmake -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER=thread ..
$ make -j`nproc`
```

There are some differences between macOS and Linux (i.e., mutex behavior) that might cause test cases
to produce different results in different platforms. We recommend using a Linux VM for running
test cases and reproducing errors whenever possible.
