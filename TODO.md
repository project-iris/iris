  Iris - Distributed Messaging Framework - Todo list
======================================================

Stuff that need implementing, fixing or testing.

- Features
    - Overlay
        - Limit number of parallel incoming STS handshakes (CPU exhaustion)
        - Proximity features for Pastry
        - Convergence check to remove annoying sleeps from tests. 
- Bugs
- Misc
    - Overlay
        - Benchmark and tune the handshakes
        - Benchmark and tune the state maintenance and updates
        - Benchmark and tune the routing performance
