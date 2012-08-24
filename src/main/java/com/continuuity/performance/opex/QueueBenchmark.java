package com.continuuity.performance.opex;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.performance.benchmark.*;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

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
    QueueEnqueue enqueue = new QueueEnqueue(queueBytes, value);
    if (!opex.execute(enqueue))
      throw new BenchmarkException("Operation " + enqueue + " failed.");
  }

  void doDequeue(int consumerId) throws BenchmarkException {

    // create a dequeue operation
    QueueConsumer consumer = new QueueConsumer(consumerId, 1, numConsumers);
    QueueConfig config = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), numPendingAcks == 0);
    QueueDequeue dequeue = new QueueDequeue(queueBytes, consumer, config);

    // first dequeue
    DequeueResult result = opex.execute(dequeue);
    if (!result.isSuccess()) {
      throw new BenchmarkException(
          "Operation " + dequeue + " failed: " + result.getMsg());
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
      if (!opex.execute(ackToExecute))
        throw new BenchmarkException("Operation " + ackToExecute + " failed.");
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
        opex.execute(ack);
        // ignore success or failure
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
