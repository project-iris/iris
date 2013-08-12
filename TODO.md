  Iris - Distributed Messaging Framework - Todo list
======================================================

Stuff that need implementing, fixing or testing.

- Planned
    - Gather and display statistics (small web server + stats publish)
- Features
    - Relay + Iris
        - Remove goroutine / pending request (either limit max requests or completely refactor proto/iris)
    - Carrier
        - Exchange topic load report only for app groups, not topics
    - Overlay
        - Limit number of parallel incoming STS handshakes (CPU exhaustion)
        - Proximity features for Pastry?
        - Convergence check to remove annoying sleeps from tests
    - Bootstrapper
        - Replace the custom hacked protocol with gobber
    - Session
        - Memory pool to reduce GC overhead (maybe will need larger refactor)
- Bugs
    - Thread pool
        - Terminate does not wait for threads to finish
    - Relay
        - Race condition if reply and immediate close (needs close sync with finishing ops)
    - Iris
        - Detect dead tunnel (heartbeat or topic-style node monitoring?)
- Misc
    - Overlay
        - Benchmark and tune the handshakes
        - Benchmark and tune the state maintenance and updates
        - Benchmark and tune the routing performance
- Upstream Go bugs:
    - Slice corruption with 64 bit indices on 386
        - Link: https://code.google.com/p/go/issues/detail?id=5820
        - Hack: slice[int(index_64)] = nil
        - Used: proto/iris/tunnel.go x2

