  Iris - Distributed Messaging Framework - Todo list
======================================================

Stuff that need implementing, fixing or testing.

- Features
    - Carrier
        - Use full topic tree (midway terminations) for publishing too (like balancing)
    - Overlay
        - Limit number of parallel incoming STS handshakes (CPU exhaustion)
        - Proximity features for Pastry
        - Convergence check to remove annoying sleeps from tests
    - Bootstrapper
        - Replace the custom hacked protocol with gobber
    - Session
        - Memory pool to reduce GC overhead (maybe will need larger refactor)
        - Hide Header.Mac into Header.Meta
    - System
        - CPU usage measurements for darwin and windows
- Bugs
- Misc
    - Overlay
        - Benchmark and tune the handshakes
        - Benchmark and tune the state maintenance and updates
        - Benchmark and tune the routing performance
