  Iris - Distributed Messaging Framework - Todo list
======================================================

Stuff that need implementing, fixing or testing.

- Planned
    - Gather and display statistics (small web server + stats publish)
- Features
    - Carrier + Overlay
        - Implement proper statistics gathering and reporting mechanism (and remove them from the Boot func)
    - Relay + Iris
        - Remove goroutine / pending request (either limit max requests or completely refactor proto/iris)
    - Carrier
        - Exchange topic load report only for app groups, not topics
    - Session
        - Memory pool to reduce GC overhead (maybe will need larger refactor)
- Bugs
    - Relay
        - Race condition if reply and immediate close (needs close sync with finishing ops)
    - Overlay
        - Proper closing and termination (i.e. try and minimize lost messages when closing)
