Code to load and run scripts as [Service Abstraction Layer (SAL) components](https://docushare.lsstcorp.org/docushare/dsweb/Get/Document-21527/).

The primary classes are:
* `scriptloader.ScriptLoader`: a Commandable SAL Component (SCS) that loads and configures Scripts that are SAL components.
* `scriptloader.BaseScript`: a base class for scripts loaded by `scriptloader.ScriptLoader`.

The package is compatible with LSST DM's `scons` build system and `eups` package management system.
Assuming you have the basic LSST DM stack installed you can do the following, from within the package directory:

- `setup -r .` to setup the package and dependencies.
- `scons` to build the package and run unit tests.
- `scons install declare` to install the package and declare it to eups.
- `package-docs build` to build the documentation.
  This requires optional [dependencies](https://developer.lsst.io/stack/building-single-package-docs.html) beyond those required to build and use the package.
