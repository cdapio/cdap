/**
 * This package implements named table using {@link com.continuuity.api.data.DataSet} APIs.
 * A table can execute read, write, delete operations. The operations can happen synchronously or asynchronously:
 * <li>Synchronously: The operation is executed immediately against the
 *   data fabric, in its own transaction. This is supported for all types
 *   of operations. </li>
 * <li>Asynchronously: The operation is staged for execution as part of
 *   the transaction of the context in which this data set was
 *   instantiated (a flowlet, or a procedure). In this case,
 *   the actual execution is delegated to the context. This is useful
 *   when multiple operations, possibly over multiple table,
 *   must be performed atomically. This is only supported for write
 *   operations.</li>
 *
 */
package com.continuuity.api.data.dataset.table;
