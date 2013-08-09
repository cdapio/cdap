package com.continuuity.metrics.process;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.transport.MetricsRecord;

import java.util.Iterator;

/**
 * For accepting and processing {@link MetricsRecord}.
 */
public interface MetricsProcessor {

  void process(MetricsScope scope, Iterator<MetricsRecord> records);
}
