/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;

/**
 * A OVCTableHandle that supports TTL.
 * This is a hack over current API. Needs refactor later.
 */
public interface TimeToLiveOVCTableHandle extends OVCTableHandle {

  OrderedVersionedColumnarTable getTable(byte [] tableName, int ttl) throws OperationException;
}
