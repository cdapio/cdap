/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.io;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.QueueSpecification;
import com.continuuity.api.flow.QueueSpecificationGenerator;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.api.io.UnsupportedTypeException;
import com.google.common.base.Throwables;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 */
public class SimpleQueueSpecificationGenerator implements QueueSpecificationGenerator {
  private final SchemaGenerator generator;
  private final FlowSpecification specification;
  private final FlowletDefinition definition;

  public SimpleQueueSpecificationGenerator(SchemaGenerator generator, FlowSpecification specification,
                                           FlowletDefinition definition) {
    this.generator = generator;
    this.specification = specification;
    this.definition = definition;
  }

  @Override
  public QueueSpecification generate(String key, Type type) throws UnsupportedTypeException {
    try {
      Schema schema = generator.generate(type);
      URI uri = new URI("queue", this.specification.getName(), "/" + definition.getFlowletSpec().getName() + "/" + key, null);
      QueueSpecification queueSpecification = new SimpleQueueSpecification(uri, schema);
      return queueSpecification;
    } catch (URISyntaxException e) {
      Throwables.propagate(e);
    }
    return null;
  }
}
