# Kafka state-store issue

## Issue

Even with exactly-once delivery enabled, the state-store is not kep in sync with records being processed.

Consider the following:

1. record N received
1. record's identity saved to state store (for business-level deduplication)
1. processing throws an exception, killing the node and ensuring no outgoing records are sent
    
### Expected behaviour:
1. Node is restarted
1. Offset was never updated, so record N is reprocessed
1. State-store is reset to position N-1
1. Record is reprocessed
    
### Actual Behaviour
1. Node is restarted
1. Record N is reprocessed (good)
1. The state store has the state from the previous processing
1. sad :(

In this case, the state-store causes the app to think the re-run message is a duplicate, when it obviously is not 

## Proof-of-concept

n.b. to clear down the rocksdb storage, run `make reset`

In at least 3 consoles (in this order:)

1. (Console 1) `make kafka` Spins up a docker-compose with Kafka and Zookeeper, configured for exactly-once
1. (Console 2) `make topology` The app will initialise its topics and await records
1. (Console 3) `make client` Posts a record to Kafka - just a Guid/Guid pair - the deduplication key
1. *The topology  will crash..*
1. (Console 2) `make topology` The app will re-process the message, but will recognise it already from last time...


