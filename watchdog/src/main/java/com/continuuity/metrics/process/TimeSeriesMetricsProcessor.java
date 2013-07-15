package com.continuuity.metrics.process;

import com.continuuity.api.data.OperationException;
import com.continuuity.metrics.data.TimeSeriesTable;
import com.continuuity.metrics.transport.MetricsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A {@link MetricsProcessor} that writes metrics into time series table. It ignore write errors by simply
 * logging the error and proceed.
 */
public final class TimeSeriesMetricsProcessor implements MetricsProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesMetricsProcessor.class);

  private final TimeSeriesTable timeSeriesTable;

  public TimeSeriesMetricsProcessor(TimeSeriesTable timeSeriesTable) {
    this.timeSeriesTable = timeSeriesTable;
  }

  @Override
  public void process(Iterator<MetricsRecord> records) {
    try {
      timeSeriesTable.save(records);
    } catch (OperationException e) {
      LOG.error("Failed to write to time series table: {}", e.getMessage(), e);
    }
  }
}
