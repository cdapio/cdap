/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.process;

import java.util.Set;

/**
 * Factory for creating {@link KafkaMetricsProcessorService}.
 */
public interface KafkaMetricsProcessorServiceFactory {

  KafkaMetricsProcessorService create(Set<Integer> partitions);
}
