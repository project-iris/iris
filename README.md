  Iris - Decentralized cloud messaging
========================================

Iris is an attempt at bringing the simplicity and elegance of cloud computing to the application layer. Consumer clouds provide unlimited virtual machines at the click of a button, but leaves it to developer to wire them together. Iris ensures that you can forget about networking challenges and instead focus on solving your own domain problems.

It is a completely decentralized messaging solution for simplifying the design and implementation of cloud services. Among others, Iris features zero-configuration (i.e. start it up and it will do its magic), semantic addressing (i.e. application use textual names to address each other), clusters as units (i.e. automatic load balancing between apps of the same name) and perfect secrecy (i.e. all network traffic is encrypted).

You can find further infos on the [Iris website](http://iris.karalabe.com) and details of the above features in the [Core concepts](http://iris.karalabe.com/book/core_concepts) section of [The book of Iris](http://iris.karalabe.com/book). For the scientifically inclined, a small collection of [papers](http://iris.karalabe.com/papers) is also available featuring Iris.

There is a growing community on Twitter [@iriscmf](https://twitter.com/iriscmf), Google groups [project-iris](https://groups.google.com/group/project-iris) and GitHub [project-iris](https://github.com/project-iris).

  Compatibility
-----------------

Since from time to time the relay protocol changes (communication between Iris and client libraries), the below compatibility matrix was introduced to make it easier to find which versions of the client libraries (columns) match with which versions of the Iris node (rows).

| | [iris-erl](https://github.com/project-iris/iris-erl) | [iris-go](https://github.com/project-iris/iris-go) | [iris-java](https://github.com/project-iris/iris-java) | [iris-scala](https://github.com/project-iris/iris-scala) |
|:-:|:-:|:-:|:-:|:-:|
| **v0.3.x** | [v1](https://github.com/project-iris/iris-erl/tree/v1) | [v1](https://github.com/project-iris/iris-go/tree/v1) | [v1](https://github.com/project-iris/iris-java/tree/v1) | [v1](https://github.com/project-iris/iris-scala/tree/v1) |
| **v0.2.x** | [v0](https://github.com/project-iris/iris-erl/tree/v0) | [v0](https://github.com/project-iris/iris-go/tree/v0) | - | - |
| **v0.1.x** | [v0](https://github.com/project-iris/iris-erl/tree/v0) | [v0](https://github.com/project-iris/iris-go/tree/v0) | - | - |

  Releases
------------

 * Development:
    - Fix Google Compute Engine netmask issue (i.e. retrieve real network configs).
    - Seamlessly use local CoreOS/etcd service as bootstrap seed server.
 * Version 0.3.2: **October 4, 2014**
    - Use 4x available CPU cores by default (will need a flag for this later).
 * Version 0.3.1: **September 22, 2014**
    - Open local relay endpoint on both IPv4 and IPv6 (bindings can remain oblivious).
    - Fix bootstrap crash in case of single-host networks (host space < 2 bits).
    - Fix race condition between tunnel construction request and finalization.
 * Version 0.3.0: **August 11, 2014**
    - Work around upstream Go bug [#5395](http://code.google.com/p/go/issues/detail?id=5395) on Windows.
    - Fix memory leak caused by unreleased connection references.
    - Fix tunnel lingering caused by missing close invocation in Iris overlay.
    - Fix message loss caused by clearing scheduled messages during a relay close.
    - Fix race condition between tunnel construction and operation.
    - Rewrite relay protocol to v1.0-draft2.
       - Proper protocol negotiation (magic string, version numbers).
       - Built in error fields to remote requests, no need for user wrappers.
       - Tunnel data chunking to support arbitrarily large messages.
       - Size based tunnel throttling opposed to the message count previously.
    - Migrate from github.com/karalabe to github.com/project-iris.
 * Version 0.2.0: **March 31, 2014**
    - Redesigned tunnels based on direct TCP connections.
    - Prioritized system messages over separate control connections.
    - Graceful connection and overlay tear-downs (still plenty to do).
    - Countless stability fixes (too many to enumerate)
 * Version 0.1-pre2 (hotfix): **September 11, 2013**
    - Fix fast subscription reply only if subscription succeeds.
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

Due to the already significant complexity of the project, I kindly ask anyone willing to pinch in to first file an [issue](https://github.com/project-iris/iris/issues) with their plans to achieve a best possible integration :).

Additionally, to prevent copyright disputes and such, a signed contributor license agreement is required to be on file before any material can be accepted into the official repositories. These can be filled online via either the [Individual Contributor License Agreement](http://iris.karalabe.com/icla) or the [Corporate Contributor License Agreement](http://iris.karalabe.com/ccla).
