package com.continuuity.performance.opex;

import com.continuuity.performance.benchmark.BenchmarkRunner;

import java.util.Arrays;

/**
 * OpexProvider class that can be used to run benchmarks against patched Mini-HBase.
 */
public class LocalNativeHBaseOpexProvider extends LocalHBaseOpexProvider {

  @Override
  protected boolean useNativeQueues() {
    return true;
  }

  public static void main(String[] args) throws Exception {
    String[] args1 = Arrays.copyOf(args, args.length + 2);
    args1[args.length] = "--opex";
    args1[args.length + 1] = LocalNativeHBaseOpexProvider.class.getName();
    BenchmarkRunner.main(args1);
  }
}
