package com.continuuity.common.utils;

import java.io.PrintStream;

public class Copyright {

  private static final String[] lines = {
      "============================================================================",
      " Continuuity BigFlow - Copyright 2012 Continuuity, Inc. All Rights Reserved.",
      ""
  };

  public static void print(PrintStream out) {
    for (String line : lines) out.println(line);
  }

  public static void print() {
    print(System.out);
  }
}
