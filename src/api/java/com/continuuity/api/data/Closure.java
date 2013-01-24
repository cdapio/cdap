package com.continuuity.api.data;

/**
 * This is a common interface for closures. A closure is a prescription for
 * some work to be done later. Closures can be used to pass around operations
 * for execution in a future context.
 *
 * One example for a closure is an increment operation whose result is stored
 * in a field of a tuple. The caller can invoke a method of a data set to
 * receive a closure, and pass that closure to the tuple builder. The
 * increment operation will then be executed immediately before the tuple is
 * built.
 */
public interface Closure {
}
