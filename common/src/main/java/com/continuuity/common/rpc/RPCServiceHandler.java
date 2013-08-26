/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.rpc;

/**
 * Defines lifecycle interface for all rpc handlers.
 */
public interface RPCServiceHandler {

  void init() throws Exception;

  void destroy() throws Exception;
}
