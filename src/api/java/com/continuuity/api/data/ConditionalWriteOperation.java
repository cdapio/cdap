package com.continuuity.api.data;

/**
 * An {@link Operation} that writes data, is atomic, but is not retryable.
 */
public interface ConditionalWriteOperation extends WriteOperation {

}
