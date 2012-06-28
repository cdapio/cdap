package com.continuuity.common.service.distributed;

/**
 *
 *
 */
public interface MasterConnectionHandler<T> {
  T connect();
}
