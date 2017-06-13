package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.internal.schedule.trigger.TriggerBuilder;
import co.cask.cdap.proto.id.DatasetId;

/**
 * A Trigger builder that builds a PartitionTrigger.
 */
public class PartitionTriggerBuilder implements TriggerBuilder {
  private DatasetId dataset;
  private int numPartitions;

  public PartitionTriggerBuilder(DatasetId dataset, int numPartitions) {
    this.dataset = dataset;
    this.numPartitions = numPartitions;
  }

  @Override
  public PartitionTrigger build(String namespace, String application, String applicationVersion) {
    return new PartitionTrigger(dataset, numPartitions);
  }
}
