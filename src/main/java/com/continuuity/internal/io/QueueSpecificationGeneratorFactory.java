/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.io;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.QueueSpecificationGenerator;

/**
 *
 */
public interface QueueSpecificationGeneratorFactory {
  QueueSpecificationGenerator create(FlowSpecification spec, FlowletDefinition def);
}
