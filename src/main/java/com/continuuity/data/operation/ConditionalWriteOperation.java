package com.continuuity.data.operation;

/**
 * An {@link Operation} that writes data, is atomic, but is not retryable.
 */
public interface ConditionalWriteOperation extends WriteOperation {

}
