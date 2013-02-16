/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.io;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.QueueSpecificationGenerator;
import com.continuuity.api.io.SchemaGenerator;

/**
 *
 */
public class SimpleQueueSpecificationGeneratorFactory implements QueueSpecificationGeneratorFactory {
  private final SchemaGenerator schemaGenerator;

  public SimpleQueueSpecificationGeneratorFactory(SchemaGenerator schemaGenerator) {
    this.schemaGenerator = schemaGenerator;
  }

  @Override
  public QueueSpecificationGenerator create(FlowSpecification specification, FlowletDefinition definition) {
    return new SimpleQueueSpecificationGenerator(schemaGenerator, specification, definition);
  }

  public static SimpleQueueSpecificationGeneratorFactory create() {
    return new SimpleQueueSpecificationGeneratorFactory(new ReflectionSchemaGenerator());
  }
}
