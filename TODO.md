  Iris - Distributed Messaging Framework - Todo list
======================================================

Stuff that need implementing, fixing or testing.

- Features
    - Overlay
        - Limit number of parallel incoming STS handshakes (CPU exhaustion)
        - Proximity features for Pastry
        - Convergence check to remove annoying sleeps from tests
    - Bootstrapper
        - Replace the custom hacked protocol with gobber
    - Session
        - Memory pool to reduce GC overhead (maybe will need larger refactor)
    - System
        - CPU usage measurements for darwin and windows
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
