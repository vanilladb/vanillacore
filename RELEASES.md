
# Version 0.2.2 (2017-06-08)

## Refactoring

- Removed the old interface for initializing VanillaDb ([#9])
- Maked VanillaDb accept a StoredProcedureFactory as a parameter during initialization ([#9], [#10])

## Enhancements

- Added a debug tool, `org.vanilladb.core.util.Timer`, in order to record the running time in given components for a thread ([#9])

## Bug Fixes

- Maked `SQLIntepretor` case insensitive to `SELECT` and `EXPLAIN` ([#8])

[#8]: https://github.com/vanilladb/vanillacore/pull/8
[#9]: https://github.com/vanilladb/vanillacore/pull/9
[#10]: https://github.com/vanilladb/vanillacore/pull/10

# Version 0.2.1 (2016-09-13)

- Updated Maven configurations
- Deployed the project to Maven Central Repository
- Added Travis CI support for testing
- Fixed some bugs of ARIES-like recovery

# Version 0.2.0

- Replaced the basic recovery algorithm with ARIES-like recovery algorithm
- Added test cases for each component
- Refactored the whole project
  - Removed most unused code
  - Unified the naming of methods

# Version 0.1.0

- Basic function works.
