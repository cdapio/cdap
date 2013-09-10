package com.continuuity.performance.opex;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QueueBenchmark3.
 */
public class QueueBenchmark3 extends OpexBenchmark {
  private static final long FOREVER = Long.MAX_VALUE;

  private static final Logger Log = LoggerFactory.getLogger(QueueBenchmark3.class);

  int numProducers = 1;
  int numConsumers = 1;

  String queueName = "queue://benchmark";
  byte[] queueBytes = null;

  // this will be used by each consumer to hold back a number of delayed acks
  int numPendingAcks = 0;
  ArrayList<LinkedList<QueueAck>> pendingAcks;
  AtomicLong emptyCount = new AtomicLong(0L);
  AtomicLong failedAck = new AtomicLong(0L);
  AtomicBoolean pauseAllAgents = new AtomicBoolean(false);
  ConcurrentHashMap<BytesWrapper, Boolean> nqd = new ConcurrentHashMap<BytesWrapper, Boolean>();
  ConcurrentHashMap<Integer, Long> dqd = new ConcurrentHashMap<Integer, Long>();
  ConcurrentHashMap<Integer, Long> acked = new ConcurrentHashMap<Integer, Long>();
  AtomicLong enqueuedCount = new AtomicLong(0L);
  AtomicLong dequeuedCount = new AtomicLong(0L);
  AtomicLong ackedCount = new AtomicLong(0L);

  @Override
  public Map<String, String> usage() {
    Map<String, String> usage = super.usage();
    usage.put("--producers <num>", "Number of producer agents to run. Each producer will enqueue " +
              "runs/producers times. Default is 1.");
    usage.put("--consumers <num>", "Number of consumer agents to run. Each consumer will dequeue " +
              "runs/producers times. Default is 1.");
    usage.put("--ack <num>", "Number of runs to delay the ack for each " + "dequeue. Default is 0 (ack immediately).");
    usage.put("--queue <name>", "Name of the queue to enqueue/dequeue. " + "Default is 'queue://benchmark'.");
    return usage;
  }

  @Override
  public void configure(CConfiguration config) throws BenchmarkException {
    // configure everything else
    super.configure(config);

    // get number of consumer and producers
    numProducers = config.getInt("producers", numProducers);
    numConsumers = config.getInt("consumers", numConsumers);
    numPendingAcks = config.getInt("ack", numPendingAcks);

    // convert queue name into binary once and forever
    queueName = config.get("queue", queueName);
    queueBytes = queueName.getBytes();

    // initialize the list of pending acks for each consumer
    pendingAcks = Lists.newArrayList();
    for (int i = 0; i < numConsumers; i++) {
      pendingAcks.add(new LinkedList<QueueAck>());
    }
  }

  LinkedList<QueueAck> getPending(int agentId) {
    return pendingAcks.get(agentId);
  }
  private void pauseThread() {
    try {
      Thread.sleep(FOREVER);
    } catch (InterruptedException e1) {
    }
  }
  private void sleepThread(long milli) {
    try {
      Thread.sleep(milli);
    } catch (InterruptedException e1) {
    }
  }
  private void pauseAllAgents() {
    pauseAllAgents.compareAndSet(false, true);
    sleepThread(FOREVER);
  }
  long doEnqueue(long iteration, int agentId) throws BenchmarkException {
    if (pauseAllAgents.get()) {
      pauseThread();
    }

    byte[] value = new byte[12];
    Bytes.putInt(value, 0, agentId);
    Bytes.putLong(value, 4, iteration);
    BytesWrapper valueBytes = new BytesWrapper(value);
    QueueEnqueue enqueue = null;
    try {
      enqueue = new QueueEnqueue(queueBytes, new QueueEntry(value));
      opex.commit(opContext, enqueue);
      enqueuedCount.incrementAndGet();
      if (nqd.putIfAbsent(valueBytes, Boolean.TRUE) != null) {
        System.err.println("ERROR: Enqueued before " + enqueue);
        pauseAllAgents();
      }

    } catch (Exception e) {
      //System.err.println("Operation " + enqueue + " failed: " + e.getMessage() + "(Ignoring this error)");
      Log.error("Operation " + enqueue + " failed: " + e.getMessage() + "(Ignoring this error)");
      return 0L;
    }
    return 1L;
  }

  long doDequeue(int consumerId) throws BenchmarkException {
    if (pauseAllAgents.get()) {
      pauseThread();
    }

    // create a dequeue operation
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, numPendingAcks == 0);
    QueueConsumer consumer = new QueueConsumer(consumerId, 0, numConsumers, config);
    QueueDequeue dequeue = new QueueDequeue(queueBytes, consumer, config);

    // first dequeue
    DequeueResult result;
    try {
      result = opex.execute(opContext, dequeue);
      dequeuedCount.incrementAndGet();
      if (result.isEmpty()) {
        return 0L;
      } else {
        byte[] value = result.getEntry().getData();
        int enqueueAgentId = Bytes.toInt(value, 0, 4);
        long enqueueIteration  = Bytes.toLong(value, 4, 8);
        BytesWrapper valueBytes = new BytesWrapper(value);
        if (nqd.replace(valueBytes, Boolean.TRUE, Boolean.FALSE) != true) {
          System.err.println("ERROR: Dequeued entry has either been dequeued before or never been enqueued!" + dequeue);
          pauseAllAgents();
        }
      }
    } catch (OperationException e) {
      Log.error("Operation " + dequeue + " failed: " + e.getMessage() + "(Ignoring this error)", e);
      //System.err.println("Bad operation " + dequeue + " failed: " + e.getMessage() + "(Ignoring this error)");
      return 0L;
    }

    // create the ack operation for this dequeue
    QueueAck ack = new QueueAck(queueBytes, result.getEntryPointer(), consumer);
    // now check whether there is a pending ack that is due for execution
    QueueAck ackToExecute = null;
    LinkedList<QueueAck> pending = getPending(consumerId);
    pending.addLast(ack);
    if (pending.size() > numPendingAcks) {
      ackToExecute = pending.getFirst();
    }
    // execute the ack operation
    if (ackToExecute != null) {
      try {
        opex.commit(opContext, ackToExecute);
        ackedCount.incrementAndGet();
      } catch (OperationException e) {
        Log.error("Operation " + ackToExecute + " failed: " + e.getMessage() + "(Ignoring this error)", e);
        //MetricsFrontEndSystem.err.println("Bad operation " + ackToExecute + " failed: " + e.getMessage()
        // + "(Ignoring this error)");
        return 0L;
      }
      pending.removeFirst();
    }
    return 1L;
  }

  @Override
  public void initialize() throws BenchmarkException {
    super.initialize();

  }

  @Override
  public void warmup() throws BenchmarkException {
    System.out.println("Warmup: Done.");
  }

  @Override
  public void shutdown() {
    // perform all pending acks to leave the queue in a good state
    for (List<QueueAck> pending : pendingAcks) {
      for (QueueAck ack : pending) {
        try {
          opex.commit(opContext, ack);
        } catch (OperationException e) {
          // ignore success or failure
        }
      }
    }
    // don't forget to call the base class' shutdown (for the opex)
    super.shutdown();
  }

  @Override
  public AgentGroup[] getAgentGroups() {
    return new AgentGroup[] {

        new SimpleAgentGroup(super.simpleConfig) {
          @Override
          public String getName() {
            return "producer";
          }
          @Override
          public int getNumAgents() {
            return numProducers;
          }
          @Override
          public Agent newAgent(final int agentId, final int numAgents) {
            return new Agent(agentId) {
              @Override
              public long runOnce(long iteration) throws BenchmarkException {
                return doEnqueue(iteration, agentId);
              }
            };
          } // newAgent()
        }, // new SimpleAgentGroup()

        new SimpleAgentGroup(super.simpleConfig) {
          @Override
          public String getName() {
            return "consumer";
          }
          @Override
          public int getNumAgents() {
            return numConsumers;
          }
          @Override
          public Agent newAgent(final int agentId, final int numAgents) {
            return new Agent(agentId) {
              @Override
              public long runOnce(long iteration)
                  throws BenchmarkException {
                return doDequeue(agentId);
              }
            };
          } // newAgent()
        } // new SimpleAgentGroup()

    }; // new AgentGroup[]
  } // getAgentGroups()

  public static void main(String[] args) throws Exception {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--bench";
    args1[args.length + 1] = QueueBenchmark3.class.getName();
    BenchmarkRunner.main(args1);
  }
  private final class BytesWrapper {
    private final byte[] bytes;
    private final int hash;

    public BytesWrapper(final byte[]... bytes) {
      if (bytes == null) {
        throw new NullPointerException();
      }
      int totalSize = 0;
      for (byte[] b : bytes) {
        if (b == null) {
          throw new NullPointerException();
        }
        totalSize += b.length;
      }
      this.bytes = new byte[totalSize];
      int soFar = 0;
      for (byte[] b : bytes) {
        System.arraycopy(b, 0, this.bytes, soFar, b.length);
        soFar += b.length;
      }
      this.hash = Arrays.hashCode(this.bytes);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof BytesWrapper) && (Arrays.equals(bytes, ((BytesWrapper) o).bytes));
    }
  }
}
