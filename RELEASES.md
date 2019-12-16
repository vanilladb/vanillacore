# Release Notes

## Version 0.3.3 (2019-12-16)

All the following changes were merged in [#37].

### Refactoring

- Simplified the result sets of stored procedures

### Enhancements

- Removed unnecessary beforeFirst calls in constructors of Scans
- Made the waiting time in the test cases shorter
- Added more messages for starting up the system
- Reduced memory footprint in Buffers
- Added a new API to TransactionMgr
- Added an error check to IndexMgr

### Bug Fixes

- Corrected the implementation of ConcurrencyMgr

[#40]: https://github.com/vanilladb/vanillacore/pull/40

## Version 0.3.2 (2018-08-20)

### Bug Fixes

- Fixed the bug of read phantom that created by improper index locking. ([#37])

[#37]: https://github.com/vanilladb/vanillacore/pull/37

## Version 0.3.1 (2018-04-24)

### Bug Fixes

- Fixed the bug causing rolling back a transaction twice ([#32])
- Fixed the error in JavaDoc ([#33])
- Fixed a few bugs after restarting a system from a crash ([#34])

[#32]: https://github.com/vanilladb/vanillacore/pull/32
[#33]: https://github.com/vanilladb/vanillacore/pull/33
[#34]: https://github.com/vanilladb/vanillacore/pull/34

## Version 0.3.0 (2017-10-11)

### Enhancements

- Added DropTable, DropView and DropIndex ([#15], [#21])
- Added Selinger-like planner for query optimization ([#23])
- Added the support of indexing on multiple fields of a table ([#27])

### Code-level Improvements

- Implemented `Comparable` for `RecordId` and `BlockId` ([#18])

### Bug Fixes

- Fixed `NullPointerException` caused by `GROUP BY` query ([#16])
- Fixed duplicate-postfix problem of B-tree indexes during logging ([#20])
- Fixed the bug that uses too many threads caused by test cases ([#26])

### Others

- Added `CONTRIBUTING.md` for the newcomers to know how to contribute ([#22])
- Removed `develop` branch and updated the corresponding configuration in `.travis.yml` ([#22])

[#15]: https://github.com/vanilladb/vanillacore/pull/15
[#16]: https://github.com/vanilladb/vanillacore/pull/16
[#18]: https://github.com/vanilladb/vanillacore/pull/18
[#20]: https://github.com/vanilladb/vanillacore/pull/20
[#21]: https://github.com/vanilladb/vanillacore/pull/21
[#22]: https://github.com/vanilladb/vanillacore/pull/22
[#23]: https://github.com/vanilladb/vanillacore/pull/23
[#26]: https://github.com/vanilladb/vanillacore/pull/26
[#27]: https://github.com/vanilladb/vanillacore/pull/27

## Version 0.2.2 (2017-06-08)

### Refactoring

- Removed the old interface for initializing VanillaDb ([#9])
- Maked VanillaDb accept a StoredProcedureFactory as a parameter during initialization ([#9], [#10])

### Enhancements

- Added a debug tool, `org.vanilladb.core.util.Timer`, in order to record the running time in given components for a thread ([#9])

### Bug Fixes

- Maked `SQLIntepretor` case insensitive to `SELECT` and `EXPLAIN` ([#8])

[#8]: https://github.com/vanilladb/vanillacore/pull/8
[#9]: https://github.com/vanilladb/vanillacore/pull/9
[#10]: https://github.com/vanilladb/vanillacore/pull/10

## Version 0.2.1 (2016-09-13)

- Updated Maven configurations
- Deployed the project to Maven Central Repository
- Added Travis CI support for testing
- Fixed some bugs of ARIES-like recovery

## Version 0.2.0

- Replaced the basic recovery algorithm with ARIES-like recovery algorithm
- Added test cases for each component
- Refactored the whole project
  - Removed most unused code
  - Unified the naming of methods

## Version 0.1.0

- Basic function works.
