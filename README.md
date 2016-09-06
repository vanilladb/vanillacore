# VanillaCore

[![Build Status](https://travis-ci.org/vanilladb/vanillacore.svg?branch=master)](https://travis-ci.org/vanilladb/vanillacore)
[![GitHub license](https://img.shields.io/badge/license-apache-blue.svg?style=flat)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.vanilladb/core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.vanilladb/core)

VanillaCore is a single node, multi-threaded relational database engine that partially supports the SQL-92 standard and offers connectivity via JDBC, embedding, or (Java-based) stored procedures.

## Required Tools

You will need the following tools to compile and run this project:

- Java Development Kit 1.7 (or newer)
- Maven

## Getting Started

This tutorial will teach you how to start up a database server and interact with it.

### Compiling the source and package them to a jar

This project is a maven project. You can compile the source and package the classes to a jar file via a single command using Maven:

(Note that this command also triggers testing phase, which will run all test cases in this project. You can skip testing phase using the next command.)

```
> mvn package
```

Running the test cases may take very long time (about 3 minutes in our case). If you want to skip the testing phase, use the this command instead:

```
> mvn package -Dmaven.test.skip=true
```

The jar file will be named as `core-0.2.1.jar` and in the `target` folder of the project.

### Starting up a VanillaCore server

To start up a VanillaCore server, use the following command:

(Please replace `{DB Name}` with **your database name**, which will be the name of the folder of your database files)

```
> java -classpath core-0.2.1.jar org.vanilladb.core.server.StartUp {DB Name}
```

If it works correctly, you should see `database server ready` like this:

```
Aug 09, 2016 3:27:55 PM org.vanilladb.core.server.StartUp main
INFO: database server ready
```

After starting up, VanillaCore creates a directory named as `{DB Name}` for the databases under your home directory (which is `C:/Users/{Your Username}` in Windows, `/home/{Your Username}` in Mac or most Linux distribution).

### Using SQL Interpreter

Let’s try to connect to your database server.

The server provides a JDBC interface, which you can connect to with any JDBC client. Or, you can use our simple interpreter to send the SQL commands to the server.

To start up a SQL interpreter, use the following command:

```
> java -classpath core-0.2.1.jar org.vanilladb.core.util.ConsoleSQLInterpreter
```

Now you should see a console prompt like:

```
SQL> _
```

After you enter the SQL console, you may start to give some SQL commands for interaction with the server. VanillaCore supports basic commands defined in SQL-92 standard. To see what exactly commands you can use, please check out [VanillaDB SQL](doc/vanilladb-sql.md) document.

## System Configurations

VanillaCore provides some configurable settings. These settings can be found in properties file `vanilladb.properties` which located in `src\main\resources\org\vanilladb\core`. After you compile and package the classes using Maven, the properties file will be copied to `target\properties\org\vanilladb\core`. When a VanillaCore server starts up, it will search the properties file in `properties\org\vanilladb\core` under the same directory. Therefore, if you want to adjust the settings after packaging the classes, you have to modify the one in the `target\properties\org\vanilladb\core`.

We assume that the jar file and the properties files are always in the same directory. If they are not, VanillaCore will use the default values for all settings.

You can also put the properties file at other location. To make VanillaCore know where the file is, you need to specify the path as an argument of the JVM while starting up a server:

```
> java -Dorg.vanilladb.core.config.file={path to vanilladb.properties} -classpath core-0.2.1.jar org.vanilladb.core.server.StartUp {DB Name}
```

Remember to replace `{path to vanilladb.properties}` to **the path of the file**.

### Modifying Configurations

First, find an editor to open properties file `vanilladb.properties`. Each line in the file is a key-value pair for a configuration. To modify a configuration, just update the value behind `=`.

### Commonly Used Configurations

Here are some commonly used configurations:

VanillaCore stores records in files. To adjust the size of physical blocks used by the database (default: 4096 bytes):

```
org.vanilladb.core.storage.file.Page.BLOCK_SIZE=4096
```

To adjust the location of the database files (default: the home directory of the current user):

```
org.vanilladb.core.storage.file.FileMgr.DB_FILES_DIR=
```

To adjust the number of the buffers which cache the blocks of the files in the memory (default: 1024):

```
org.vanilladb.core.storage.buffer.BufferMgr.BUFFER_POOL_SIZE=1024
```

To enable the periodically checkpointing (default: true):

```
org.vanilladb.core.server.VanillaDb.DO_CHECKPOINT=true
```

You can find more available configurations and corresponding descriptions in `vanilladb.properties`.

## Supported Syntax

Please checkout our [VanillaDB SQL](doc/vanilladb-sql.md) document.

## Architecture Tutorials

We have a series of educational slides to make the people who are not familiar with database internal architecture understand how a database works. Here is the outline of the our slides:

- [Background](http://www.vanilladb.org/slides/Background.pdf)
	- Why relational database systems? ER- and relational-models, transactions and logical schema design and normal forms, etc.
- [Architecture overview and interfaces](http://www.vanilladb.org/slides/Architecture_and_Interfaces.pdf)
	- Client-server interfaces, embedding, storage interfaces, etc.
- Query engine
	- [Server, threads and JDBC](http://www.vanilladb.org/slides/Server_and_Threads.pdf)
		- Threads v.s. connections v.s. transactions, thread-local v.s. thread-safe components, etc.
	- [Query Processing](http://www.vanilladb.org/slides/Query_Processing.pdf)
		- SQL parsing and validation, planning, algebra, plan/scan trees, etc.
- Storage
	- [Data access and file management](http://www.vanilladb.org/slides/Data_Access_and_File_Management.pdf)
		- Block-level v.s. file-level access, O_DIRECT on Linux, etc.
	- [Memory management](http://www.vanilladb.org/slides/Memory_Management.pdf)
		- Buffering user data, write-ahead-logging (WAL), log caching, etc.
	- [Record](http://www.vanilladb.org/slides/Record_Management.pdf) and [metadata management](http://www.vanilladb.org/slides/Metadata_Management.pdf)
		- Physical schema design, efficient buffer utilization, etc.
- Transaction management
	- [Concurrency](http://www.vanilladb.org/slides/Transaction_Concurrency.pdf)
		- Strict Two-Phase Locking (S2PL), deadlock detection/avoidance, lock granularity, phantom, isolation levels, etc.
	- [Recovery](http://www.vanilladb.org/slides/Transaction_Recovery.pdf)
		- Physical logging, transaction rollback, UNDO-only recovery, UNDO-REDO recovery, logical logging, physiological logging, ARIES, checkpointing, etc.
- Efficient query processing
	- [Indexing](http://www.vanilladb.org/slides/Indexing.pdf)
		- Hash and B-tree indexing, index locking, etc.
	- Materialization and sorting (TBA)
	- Effective buffer utilization (TBA)
	- Query optimization (TBA)

## Linking via Maven

```xml
<dependency>
  <groupId>org.vanilladb</groupId>
  <artifactId>core</artifactId>
  <version>0.2.1</version>
</dependency>
```
	
## Contact Information

If you have any question, you can either open an issue here or contact [vanilladb@datalab.cs.nthu.edu.tw](vanilladb@datalab.cs.nthu.edu.tw) directly.

## License

Copyright 2016 vanilladb.org contributors

Licensed under the [Apache License 2.0](LICENSE)
