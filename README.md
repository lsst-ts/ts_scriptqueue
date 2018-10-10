A Commandable SAL Component (SCS) that loads SAL Scripts.

- `scons` to build the package and run unit tests. Note that at present most of the unit tests rely on SAL libraries for component `Test` that are specific to unix that are included in the package. We hope to manage those libraries better in the future. On macOS most tests will be skipped.
- `setup -r .` to setup the package and dependencies
- `scons` to build the package and run unit tests
- `scons install` to install the package
- `package-docs build` to build the documentation. This requires optional [dependencies](https://developer.lsst.io/stack/building-single-package-docs.html) beyond those required to build and use the package.
