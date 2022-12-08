
## AMQP 0-9-1  Implementation

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