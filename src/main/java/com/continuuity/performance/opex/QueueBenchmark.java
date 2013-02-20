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
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.performance.benchmark.Agent;
import com.continuuity.performance.benchmark.AgentGroup;
import com.continuuity.performance.benchmark.BenchmarkException;
import com.continuuity.performance.benchmark.BenchmarkRunner;
import com.continuuity.performance.benchmark.SimpleAgentGroup;
import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QueueBenchmark extends OpexBenchmark {

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
    return pendingAcks.get(agentId - 1);
  }

  void doEnqueue(long iteration) throws BenchmarkException {

    byte[] value = Bytes.toBytes(iteration);
    QueueEnqueue enqueue = null;
    try {
      enqueue = new QueueEnqueue(queueBytes, value);
      opex.commit(opContext, enqueue);
    } catch (Exception e) {
      Log.error("Operation " + enqueue + " failed: " + e.getMessage() +
          "(Ignoring this error)", e);
    }
  }

  void doDequeue(int consumerId) throws BenchmarkException {

    // create a dequeue operation
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, numPendingAcks == 0);
    QueueConsumer consumer = new QueueConsumer(consumerId, 1, numConsumers, config);
    QueueDequeue dequeue = new QueueDequeue(queueBytes, consumer, config);

    // first dequeue
    DequeueResult result;
    try {
      result = opex.execute(opContext, dequeue);
    } catch (OperationException e) {
      Log.error("Operation " + dequeue + " failed: " + e.getMessage() +
          "(Ignoring this error)", e);
      return;
    }
    if (result.isEmpty()) return;

    // create the ack operation for this dequeue
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
      } catch (OperationException e) {
        Log.error("Operation " + dequeue + " failed: " + e.getMessage() +
            "(Ignoring this error)", e);
      }
      pending.removeFirst();
    }
  }

  @Override
  public void warmup() throws BenchmarkException {
    int numEnqueues = Math.min(100, simpleConfig.numRuns);
    System.out.println("Warmup: Performing " + numEnqueues + " enqueues.");
    for (int i = 0; i < numEnqueues; i++) {
      try {
        doEnqueue(i);
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
              public void runOnce(long iteration, int agentId, int numAgents)
                  throws BenchmarkException {
                doEnqueue(iteration);
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
              public void runOnce(long iteration, int agentId, int numAgents)
                  throws BenchmarkException {
                doDequeue(agentId);
              }
            };
          } // newAgent()
        } // new SimpleAgentGroup()

    }; // new AgentGroup[]
  } // getAgentGroups()

  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--bench";
    args1[args.length + 1] = QueueBenchmark.class.getName();
    BenchmarkRunner.main(args1);
  }
}
