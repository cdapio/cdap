package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.internal.schedule.trigger.TriggerBuilder;
import co.cask.cdap.proto.id.StreamId;

/**
 * A Trigger builder that builds a StreamSizeTrigger.
 */
public class StreamSizeTriggerBuilder implements TriggerBuilder {
  private StreamId streamId;
  private int triggerMB;

  public StreamSizeTriggerBuilder(StreamId streamId, int triggerMB) {
    this.streamId = streamId;
    this.triggerMB = triggerMB;
  }

  @Override
  public StreamSizeTrigger build(String namespace, String application, String applicationVersion) {
    return new StreamSizeTrigger(streamId, triggerMB);
  }
}
