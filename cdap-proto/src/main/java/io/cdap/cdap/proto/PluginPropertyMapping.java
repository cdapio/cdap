package io.cdap.cdap.proto;

import javax.annotation.Nullable;

/**
 * The mapping between a triggering pipeline plugin property to a triggered pipeline argument.
 */
public class PluginPropertyMapping extends ArgumentMapping {
  @Nullable
  private final String stageName;

  public PluginPropertyMapping(@Nullable String stageName, @Nullable String source, @Nullable String target) {
    this(stageName, source, target, null);
  }

  public PluginPropertyMapping(@Nullable String stageName,
                               @Nullable String source,
                               @Nullable String target,
                               @Nullable TriggeringPipelineId pipelineId) {
    super(source, target, pipelineId);
    this.stageName = stageName;
  }

  /**
   * @return The name of the stage where the triggering pipeline plugin property is defined
   */
  @Nullable
  public String getStageName() {
    return stageName;
  }

  @Override
  public String toString() {
    return "PluginPropertyMapping{" +
      "source='" + getSource() + '\'' +
      ", target='" + getTarget() + '\'' +
      ", stageName='" + getStageName() + '\'' +
      ", triggeringPipelineId='" + getTriggeringPipelineId() + '\'' +
      '}';
  }
}
