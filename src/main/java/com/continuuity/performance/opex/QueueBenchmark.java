package com.continuuity.performance.opex;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.Bytes;
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

public class QueueBenchmark extends OpexBenchmark {

  private static final Logger Log =
    LoggerFactory.getLogger(QueueBenchmark.class);

  int numProducers = 1;
  int numConsumers = 1;

  String queueName = "queue://benchmark";
  byte[] queueBytes = null;

  // this will be used by each consumer to hold back a number of delayed acks
  int numPendingAcks = 0;
  ArrayList<LinkedList<QueueAck>> pendingAcks;

  @Override
  public Map<String, String> usage() {
    Map<String, String> usage = super.usage();
    usage.put("--producers <num>",
        "Number of producer agents to run. Each producer will enqueue " +
            "runs/producers times. Default is 1.");
    usage.put("--consumers <num>",
        "Number of consumer agents to run. Each consumer will dequeue " +
            "runs/producers times. Default is 1.");
    usage.put("--ack <num>", "Number of runs to delay the ack for each " +
        "dequeue. Default is 0 (ack immediately).");
    usage.put("--queue <name>", "Name of the queue to enqueue/dequeue. " +
        "Default is 'queue://benchmark'.");
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

  long doEnqueue(long iteration, int agentId) throws BenchmarkException {
    byte[] value = Bytes.toBytes(iteration);
    QueueEnqueue enqueue = null;
    try {
      enqueue = new QueueEnqueue(queueBytes, new QueueEntry(value));
      opex.commit(opContext, enqueue);
    } catch (Exception e) {
      Log.error("Operation " + enqueue + " failed: " + e.getMessage() +
          "(Ignoring this error)", e);
      System.err.println("Operation " + enqueue + " failed: " + e.getMessage() +
                           "(Ignoring this error)");
      return 0L;
    }
    return 1L;
  }

  long doDequeue(int consumerId) throws BenchmarkException {
    // create a dequeue operation
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, numPendingAcks == 0);
    QueueConsumer consumer = new QueueConsumer(consumerId, 0, numConsumers, config);
    QueueDequeue dequeue = new QueueDequeue(queueBytes, consumer, config);

    // first dequeue
    DequeueResult result;
    try {
      result = opex.execute(opContext, dequeue);
      System.err.println("Good operation " + dequeue + "returned result " + result);
    } catch (OperationException e) {
      Log.error("Operation " + dequeue + " failed: " + e.getMessage() +
          "(Ignoring this error)", e);
      System.err.println("Bad operation " + dequeue + " failed: " + e.getMessage() +
                           "(Ignoring this error)");
      return 0L;
    }
    if (result.isEmpty()) {
      System.err.println("Bad operation " + dequeue + "returned empty result" +
                           "(Ignoring this error)");
      return 0L;
    }
    QueueAck ack = new QueueAck(queueBytes, result.getEntryPointer(), consumer);
    // now check whether there is a pending ack that is due for execution
    QueueAck ackToExecute = null;
    LinkedList<QueueAck> pending = getPending(consumerId);
    pending.addLast(ack);
    if (pending.size() > numPendingAcks)
      ackToExecute = pending.getFirst();

    // execute the ack operation
    if (ackToExecute != null) {
      try {
        opex.commit(opContext, ackToExecute);
        System.err.println("Good operation " + ackToExecute + "returned.");
      } catch (OperationException e) {
        Log.error("Operation " + ackToExecute + " failed: " + e.getMessage() +
            "(Ignoring this error)", e);
        System.err.println("Bad operation " + ackToExecute + " failed: " + e.getMessage() +
        "(Ignoring this error)");
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
    int numEnqueues = Math.min(100, simpleConfig.numRuns);
    numEnqueues = 10000;
    System.out.println("Warmup: Performing " + numEnqueues + " enqueues.");
    for (int i = 0; i < numEnqueues; i++) {
      try {
        doEnqueue(i, 0);
        if (i % 10000 == 0) {
          System.out.println(i);
        }
      } catch (BenchmarkException e) {
        throw new BenchmarkException(
            "Failure after " + i + " enqueues: " + e.getMessage() , e);
      }
    }
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
              public long runOnce(long iteration, int agentId, int numAgents)
                  throws BenchmarkException {
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
              public long runOnce(long iteration, int agentId, int numAgents)
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
    args1[args.length + 1] = QueueBenchmark.class.getName();
    BenchmarkRunner.main(args1);
  }
}
