  Iris - Distributed Messaging Framework - Todo list
======================================================

Stuff that need implementing, fixing or testing.

- Features
    - Overlay
        - Limit number of parallel incoming STS handshakes (CPU exhaustion)
        - Filter bootstrap results (connect'em all is too CPU intensive)
        - Proximity features for Pastry
- Bugs
    - Overlay
        - Self connection is sometimes attempted. Should only occured with a malicious node.
- Misc
    - Overlay
        - Benchmark and tune the handshakes
        - Benchmark and tune the state maintenance and updates
        - Benchmark and tune the routing performance
