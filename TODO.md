  Iris - Distributed Messaging Framework - Todo list
======================================================

This is a collection of features that have not yet been implemented, but they should eventually been added.

- Overlay
    - Limit the state exchange threads
    - Limit number of parallel STS handshakes (CPU exhaustion)
    - Proximity features for Pastry
    - Buffered channels to handle bursts

  Fix
-------

List of known bugs to make sure they don't go missing.

- Overlay
    - Self connection is sometimes attempted. Should only occured with a malicious node.
