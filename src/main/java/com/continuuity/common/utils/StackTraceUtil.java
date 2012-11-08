package com.continuuity.common.utils;

/**
 * Utilities for stack trace.
 */
public class StackTraceUtil {

  /**
   * Converts an exception into a string representation to be used for
   * printing in logs.
   *
   * @param e Exception
   * @return String representation of stack trace.
   */
  public static String toStringStackTrace(Exception e) {
    StringBuilder sb = new StringBuilder();
    StackTraceElement[] elements = e.getStackTrace();
    for(StackTraceElement element : elements) {
      sb.append(element.toString() + "\n");
    }
    return sb.toString();
  }

  /**
   * Converts an throwable into a string representation to be used for
   * printing in logs.
   *
   * @param e Exception
   * @return String representation of stack trace.
   */
  public static String toStringStackTrace(Throwable e) {
    StringBuilder sb = new StringBuilder();
    StackTraceElement[] elements = e.getStackTrace();
    for(StackTraceElement element : elements) {
      sb.append(element.toString() + "\n");
    }
    return sb.toString();
  }
}
