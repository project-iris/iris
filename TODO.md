  Iris - Distributed Messaging Framework - Todo list
======================================================

Stuff that need implementing, fixing or testing.

- Features
    - Overlay
        - Limit the state exchange threads
        - Limit number of parallel STS handshakes (CPU exhaustion)
        - Proximity features for Pastry
        - Buffered channels to handle bursts
- Bugs
    - Overlay
        - Self connection is sometimes attempted. Should only occured with a malicious node.
- Misc
    - Overlay
        - Benchmark and tune the handshakes
        - Benchmark and tune the state maintenance and updates
        - Benchmark and tune the routing performance
