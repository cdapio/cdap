package com.continuuity.performance.opex;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Bytes;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueAdmin;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueueBenchmark4 extends OpexBenchmark {
  private static long FOREVER = Long.MAX_VALUE;

  private static final Logger Log =
    LoggerFactory.getLogger(QueueBenchmark4.class);

  int numProducers = 1;
  int numConsumers = 1;

  String queueName = "queue://benchmark";
  byte[] queueBytes = null;

  // this will be used by each consumer to hold back a number of delayed acks
  int numPendingAcks = 0;
  ArrayList<LinkedList<QueueAck>> pendingAcks;
  AtomicBoolean pauseAllAgents = new AtomicBoolean(false);
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
    if (pauseAllAgents.get()==true) {
      pauseThread();
    }
    byte[] value = new byte[12];
    Bytes.putInt(value,0,agentId);
    Bytes.putLong(value, 4, iteration);
    QueueEnqueue enqueue = null;
    try {
      enqueue = new QueueEnqueue(queueBytes, new QueueEntry(value));
      opex.commit(opContext, enqueue);
      enqueuedCount.incrementAndGet();

    } catch (Exception e) {
      System.err.println("Operation " + enqueue + " failed: " + e.getMessage() + "(Ignoring this error)");
      return 0L;
    }
    return 1L;
  }

  QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, numPendingAcks == 0);

  long doDequeue(int consumerId) throws BenchmarkException {
    if (pauseAllAgents.get()==true) {
      pauseThread();
    }

    // create a dequeue operation
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
      }
    } catch (OperationException e) {
      Log.error("Operation " + dequeue + " failed: " + e.getMessage() + "(Ignoring this error)", e);
      System.err.println("Bad operation " + dequeue + " failed: " + e.getMessage() + "(Ignoring this error)");
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
        System.err.println("Bad operation " + ackToExecute + " failed: " + e.getMessage() + "(Ignoring this error)");
        return 0L;
      }
      pending.removeFirst();
    }
    return 1L;
  }

  @Override
  public void initialize() throws BenchmarkException {
    super.initialize();
    try {
      opex.execute(opContext, null,
                   new QueueAdmin.QueueConfigure(queueBytes, new QueueConsumer(0, 0, numConsumers, config)));
    } catch (OperationException e) {
      throw new BenchmarkException("Exception while configuring queue", e);
    }
  }

  @Override
  public void warmup() throws BenchmarkException {
    System.out.println("Warmup: Done.");
  }

  @Override
  public void shutdown() throws BenchmarkException {
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
          public Agent newAgent() {
            return new Agent() {
              @Override
              public long runOnce(long iteration, int agentId, int numAgents) throws BenchmarkException {
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
          public Agent newAgent() {
            return new Agent() {
              @Override
              public long warmup(int agentId, int numAgents) throws BenchmarkException {
                try {
                  Thread.sleep(1200*1000); // 2 min
                } catch (InterruptedException e) {
                  throw new BenchmarkException(e.getMessage(), e);
                }
                return 0L;
              }
              @Override
              public long runOnce(long iteration, int agentId, int numAgents)
                  throws BenchmarkException {
                return doDequeue(agentId);
              }
            };
          } // newAgent()
        } // new SimpleAgentGroup()

    }; // new AgentGroup[]
  } // getAgentGroups()

  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--bench";
    args1[args.length + 1] = QueueBenchmark4.class.getName();
    BenchmarkRunner.main(args1);
  }
}
