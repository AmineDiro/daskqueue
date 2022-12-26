# AMQP 0-9-1  Implementation

## Messages
- Upgrade Messages protocol
- Add topic support

## Exchanges / QueuePool
- Take msg and route it into 0..n queues based on the msg key
- Msg with key routing R goes to Queue(R)
- Implement Drop message /return to the publisher when we can't route

## Queues
- Store messages
- Name < 255bytes UTF-8
- Durability :  [Durable | Transient]
    - Metadata of  queue are stored on disk
    - Messages of the durable queue are stored on  disk
- Should separate queue serving and storage: the Queue Pool should provide us with a path at spawn time
- Durable queues should keep a log and flush it to disk in the background (WAL style) -> Use sqlite as a backend
- Message TTL
- Queue should keep records of poped unacked messages and either : flag them is they are acked or push them back to queue is they fail
- Should keep note of

## Consumers
- Support for message acking : [ Early Ack  | LATE ack]
- Early acks when the consumer gets the msg from the queue
- Late acks when message succeeds
- Two ways to consume: Push and Pull. For now, only support Pull style.
- Should run the pulling in a separate asyncio event loop.

---------------------------------------------------------------
# Kafka storage

- Logfile is **append only**
- Each log is split into segments
- Each Log holds up to : 1 GB of data (or time limit )
- When log is full we close it and open a new logfile

Example :
[Seg0:  0-957 (RO)] [Seg1: 958-1484 (RO)] [Seg2: 1485-... (RW)]

- [ ] Seek to last written position when we reopen the file

### Directory structure
- 1 dir per Topic ( per exchange)
  - Segment Filename is offset
  - Index | Log ( ==  segment) | TimeIndex

## Indices:
- Offset index : Index on msg offset in a specific segment : can be hashtable because msg ids are random uuids ie no order needed
- Timestamp index :  Index on timestamps to find specific msg : should be implemented as a binary search tree

## Message Status:

- A Msg can either be : Ready |  Delivered | Acked
-
