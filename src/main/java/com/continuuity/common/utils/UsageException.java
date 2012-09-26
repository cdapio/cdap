package com.continuuity.common.utils;

/**
 * This exception is only for bailing out of a command line tool when
 * the command line arguments are not valid. In that case, we want to
 * print a usage statement, but not an exception, and we also don't want
 * to call System.exit(). So, when the program detects an error, it prints
 * the help text and throws this exception. At the top level, we can catch
 * this and exit without printing. We could use IllegalArgumentException
 * instead, but we never know whether that was thrown somewhere else,
 * in which case we want to print the exception.
 */
public class UsageException extends RuntimeException {
  // no message, no cause, on purpose, only default constructor
  public UsageException() { }
}
