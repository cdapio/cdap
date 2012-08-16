package com.continuuity.data.operation.executor.benchmark;

import com.continuuity.api.data.Write;
import com.continuuity.data.operation.executor.OperationExecutor;

import java.util.ArrayList;
import java.util.Arrays;

public class WriteBenchmark extends Benchmark {

  int numWrites = 10000;
  int numThreads = 10;

  void performNWrites(OperationExecutor opex, String name, int threadId, int numWrites) {
    final byte[] key = ("key" + threadId).getBytes();
    System.out.println(name + " " + threadId +
        ": Performing " + numWrites + " writes to key " +
        new String(key) + " with opex: " + opex.getName());
    final byte[] value = { 0 };
    Write write = new Write(key, value);
    for (int i = 0; i < numWrites; i++) {
      value[0] = (byte)(i%0x000000ff);
      if (!opex.execute(write))
        System.err.println(name + " " + threadId + ": Write " + i + " Failed.");
    }
  }

  @Override
  public String[] configure(String[] args) throws BenchmarkException {
    // --ops <n> --threads <n>
    ArrayList<String> remaining = new ArrayList<String>();
    for (int i = 0; i < args.length; i++) {
      if ("--ops".equals(args[i])) {
        if (i + 1 < args.length) {
          numWrites = Integer.valueOf(args[++i]);
          if (numWrites < 1)
            throw new BenchmarkException(
                "--ops must be a positive number" + ".");
        } else throw new BenchmarkException(
            "--ops must have an argument. ");
      }
      else if ("--threads".equals(args[i])) {
        if (i + 1 < args.length) {
          numThreads = Integer.valueOf(args[++i]);
          if (numThreads < 1)
            throw new BenchmarkException(
                "--threads must be a positive number.");
        } else throw new BenchmarkException(
            "--threads must have an argument. ");
      } else
        remaining.add(args[i]);
    }
    return remaining.toArray(new String[remaining.size()]);
  }

  @Override
  public void warmup(OperationExecutor opex) {
    performNWrites(opex, "warmup", 0, numWrites);
  }

  @Override
  public AgentGroup[] getGroups(OperationExecutor opex) {
    AgentGroup[] groups = new AgentGroup[1];
    groups[0] = new WriteGroup();
    return groups;
  }

  class WriteGroup extends AgentGroup {

    @Override
    public String getName() {
      return "Write";
    }

    @Override
    public int getNumInstances() {
      return numThreads;
    }

    @Override
    public Runnable getAgent(final OperationExecutor opex, final int instanceInGroup) {
      return new Runnable() {
        @Override
        public void run() {
          performNWrites(opex, getName(), instanceInGroup, numWrites);
        }
      };
    }

  } // WriteGroup

  public static void main(String[] args) {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--benchmark";
    args1[args.length + 1] = WriteBenchmark.class.getName();
    BenchmarkRunner.main(args1);
  }

} // WriteBenchmark
