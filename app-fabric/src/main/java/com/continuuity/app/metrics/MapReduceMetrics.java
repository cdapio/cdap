package com.continuuity.app.metrics;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsScope;

/**
 * Metrics collector for MapReduce job.
 */
public final class MapReduceMetrics extends AbstractProgramMetrics {

  /**
   * Type of map reduce task.
   */
  public enum TaskType {
    Mapper("m"),
    Reducer("r");

    private final String id;

    private TaskType(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }
  }

  public MapReduceMetrics(MetricsCollectionService collectionService, String applicationId,
                          String mapReduceId, TaskType type) {
    super(collectionService.getCollector(
      MetricsScope.USER,
      String.format("%s.b.%s.%s", applicationId, mapReduceId, type.getId()), "0"));
  }
}
