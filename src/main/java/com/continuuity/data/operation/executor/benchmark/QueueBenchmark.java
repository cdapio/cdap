package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class QueueBenchmark extends Benchmark {

  int numEvents = 10000;
  int numProducers = 4;
  int numConsumers = 5;
  int numPendingAcks = 0;
  int reportInterval = 0;
  String queueName = "queue://benchmark";

  @Override
  public String[] configure(String[] args) throws BenchmarkException {
    // --events <n> --consumers <n> --producers<n> --ack <n>
    ArrayList<String> remaining = new ArrayList<String>();
    for (int i = 0; i < args.length; i++) {
      if ("--events".equals(args[i])) {
        if (i + 1 < args.length) {
          numEvents = Integer.valueOf(args[++i]);
          if (numEvents < 1)
            throw new BenchmarkException(
                "--events must be a positive number.");
        } else throw new BenchmarkException(
            "--events must have an argument. ");
      }
      else if ("--consumers".equals(args[i])) {
        if (i + 1 < args.length) {
          numConsumers = Integer.valueOf(args[++i]);
          if (numConsumers < 1)
            throw new BenchmarkException(
                "--consumers must be a positive number.");
        } else throw new BenchmarkException(
            "--consumers must have an argument. ");
      }
      else if ("--producers".equals(args[i])) {
        if (i + 1 < args.length) {
          numProducers = Integer.valueOf(args[++i]);
          if (numProducers < 1)
            throw new BenchmarkException(
                "--producers must be a positive number.");
        } else throw new BenchmarkException(
            "--producers must have an argument. ");
      }
      else if ("--ack".equals(args[i])) {
        if (i + 1 < args.length) {
          numPendingAcks = Integer.valueOf(args[++i]);
          if (numPendingAcks < 0)
            throw new BenchmarkException(
                "--ack must be an unsigned number.");
        } else throw new BenchmarkException(
            "--ack must have an argument. ");
      }
      else if ("--queue".equals(args[i])) {
        if (i + 1 < args.length)
          queueName = args[++i];
        else throw new BenchmarkException(
            "--queue must have an argument. ");
      }
      else if ("--interval".equals(args[i])) {
        if (i + 1 < args.length)
          queueName = args[++i];
        else throw new BenchmarkException(
            "--queue must have an argument. ");
      }
      else
        remaining.add(args[i]);
    }
    return remaining.toArray(new String[remaining.size()]);
  }

  @Override
  public void warmup(OperationExecutor opex) {
    performNEnqueues(opex, "Warmup", 0, Math.max(numEvents / 4, 200));
  }

  @Override
  public AgentGroup[] getGroups(OperationExecutor opex) {
    AgentGroup[] groups = new AgentGroup[2];
    groups[0] = new ProducerGroup();
    groups[1] = new ConsumerGroup();
    return groups;
  }

  class ProducerGroup extends AgentGroup {

    @Override
    public String getName() {
      return "Producer";
    }

    @Override
    public int getNumInstances() {
      return numProducers;
    }

    @Override
    public Runnable getAgent(final OperationExecutor opex,
                             final int instanceInGroup) {
      return new Runnable() {
        @Override
        public void run() {
          performNEnqueues(
              opex, getName(), instanceInGroup, numEvents / numProducers);
        }
      };
    }

  } // ProducerGroup

  class ConsumerGroup extends AgentGroup {

    @Override
    public String getName() {
      return "Consumer";
    }

    @Override
    public int getNumInstances() {
      return numConsumers;
    }

    @Override
    public Runnable getAgent(
        final OperationExecutor opex, final int instanceInGroup) {
      return new Runnable() {
        @Override
        public void run() {
          performNDequeues(
              opex, getName(), instanceInGroup, numEvents / numConsumers);
        }
      };
    }

  } // ProducerGroup

  void performNEnqueues(OperationExecutor opex,
                        String name, int threadId, int numOps) {

    final byte[] q = (queueName).getBytes();
    final byte[] value = { 0 };
    QueueEnqueue enqueue = new QueueEnqueue(q, value);

    System.out.println(name + " " + threadId +
        ": Performing " + numOps + " enqueues to queue " +
        queueName + " with opex: " + opex.getName());

    long start = System.currentTimeMillis();

    for (int i = 0; i < numOps; i++) {
      if (!opex.execute(enqueue))
        System.err.println(name + " " + threadId + ": Enqueue Failed. ");
    }

    long end = System.currentTimeMillis();
    long time = end - start;
    System.out.println(name + " " + threadId + ": Done with " + numOps + " " +
        "enqueues after " + time + " ms (" + ((float)numOps * 1000 / time) +
        "/sec)");
  }

  void performNDequeues(OperationExecutor opex,
                        String name, int threadId, int numOps) {

    final byte[] q = (queueName).getBytes();
    QueueConsumer consumer = new QueueConsumer(threadId, 1, numConsumers);
    QueueConfig config = new QueueConfig(
        new QueuePartitioner.RandomPartitioner(), numPendingAcks == 0);
    QueueDequeue dequeue = new QueueDequeue(q, consumer, config);
    LinkedList<QueueAck> pendingAcks = new LinkedList<QueueAck>();

    System.out.println(name + " " + threadId + ": Performing " + numOps +
        " dequeues and acks (delayed by " + numPendingAcks + ") to queue " +
        queueName + " with opex: " + opex.getName());

    long start = System.currentTimeMillis();

    for (int i = 0; i < numOps; i++) {

      // first dequeue
      DequeueResult result = opex.execute(dequeue);
      if (!result.isSuccess()) {
        System.err.println(name + " " + threadId +
            ": Dequeue Failed - " + result.getMsg());
        continue;
      }

      // now check whether there is a pending ack that is due for execution
      QueueAck ack = new QueueAck(q, result.getEntryPointer(), consumer);
      QueueAck ackToExecute = null;
      if (numPendingAcks == 0) {
        ackToExecute = ack;
      } else {
        pendingAcks.addLast(ack);
        if (pendingAcks.size() > numPendingAcks)
          ackToExecute = pendingAcks.removeFirst();
      }

      if (ackToExecute != null) {
        if (!opex.execute(ackToExecute))
          System.err.println(name + " " + threadId + ": Ack Failed. ");
      }
    }

    // all dequeues are done, finish by executing all pending acks
    while (!pendingAcks.isEmpty()) {
      QueueAck ack = pendingAcks.removeFirst();
      if (!opex.execute(ack))
        System.err.println(name + " " + threadId + ": Ack Failed. ");
    }

    long end = System.currentTimeMillis();
    long time = end - start;
    System.out.println(name + " " + threadId + ": Done with " + numOps + " " +
        "dequeues after " + time + " ms (" + ((float)numOps * 1000 / time) +
        "/sec)");
  }

  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--benchmark";
    args1[args.length + 1] = QueueBenchmark.class.getName();
    BenchmarkRunner.main(args1);
  }

} // WriteBenchmark
