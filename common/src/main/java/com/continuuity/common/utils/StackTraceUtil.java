package com.continuuity.common.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utilities for stack trace.
 */
public class StackTraceUtil {

  /**
   * Converts an throwable into a string representation to be used for
   * printing in logs.
   *
   * @param e Exception
   * @return String representation of stack trace.
   */
  public static String toStringStackTrace(Throwable e) {
    StringWriter writer = new StringWriter();
    PrintWriter printer = new PrintWriter(writer);
    e.printStackTrace(printer);
    printer.close();
    return writer.toString();
  }
}
