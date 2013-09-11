  Iris - Decentralized Messaging Framework
============================================

  Releases
------------

 * Version 0.1-pre2 (hotfix): **development**
    - Fix fast subscription reply only if subsciption succeeds.
    - Fix topic self report after a node failure.
    - Fix heart mechanism to report errors not panics, check duplicate monitoring.
    - Fix late carrier heartbeat startup.
    - Fix panic caused by balance requests pending during topic termination.
    - Fix corrupt topic balancer caused by stale parent after removal.
 * Version 0.1-pre: **August 26, 2013**
    - Initial RFC release.

  Contributions
-----------------

Currently my development aims are to stabilize the project and its language bindings. Hence, although I'm open and very happy for any and all contributions, the most valuable ones are tests, benchmarks and bug-fixes.

Due to the already significant complexity of the project, I kindly ask anyone willing to pinch in to first file an [issue](https://github.com/karalabe/iris/issues) with their plans to achieve a best possible integration :).

Additionally, to prevent copyright disputes and such, a signed contributor license agreement is required to be on file before any material can be accepted into the official repositories. These can be filled online via either the [Individual Contributor License Agreement](http://iris.karalabe.com/icla) or the [Corporate Contributor License Agreement](http://iris.karalabe.com/ccla).
