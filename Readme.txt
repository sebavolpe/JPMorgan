Personal solution notes for Resource Scheduler exercise: 

I used two different instance of threads to make a elegant solutions.
One line of threads that will be used for the engineer. At this core I put the whole development to process the arrival messages, emulate the queuing and I created two different strategies to process the the prioritization of the message and then the sending process to the available gateway.
The other lines of thread will be create to simulate the "Third Party" mainframe consume service.

Since the gateway is asynchronous by definition (we need to wait until the completion message is called),
I had to convert it to synchronize using launching the a new thread for processing in the gateway.
 
I used naming conventions (clean code) as a good practice development. I put useful comments to help the following process.

Documentation that I used:
concurrent api java

Class ThreadPoolExecutor for the queuing
https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ThreadPoolExecutor.html#ThreadPoolExecutor-int-int-long-java.util.concurrent.TimeUnit-java.util.concurrent.BlockingQueue-java.util.concurrent.RejectedExecutionHandler-

for the architecture used: Class PriorityBlockingQueue<E>
https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/PriorityBlockingQueue.html#PriorityBlockingQueue-int-java.util.Comparator-




External: Log4j

For "production quality" I think possible improvements:

Use spring to made the strategies with beans
Use Mule ESB(Enterprise Server Bus) or spring integrations.
Use a pool for cancelled or terminated msg. Then remove from that queuing just for performance.


