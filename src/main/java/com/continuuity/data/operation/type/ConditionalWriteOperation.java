package com.continuuity.data.operation.type;

/**
 * An {@link Operation} that writes data, is atomic, but is NOT retryable.
 */
public interface ConditionalWriteOperation extends WriteOperation {

}
