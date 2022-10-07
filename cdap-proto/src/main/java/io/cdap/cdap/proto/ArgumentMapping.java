package io.cdap.cdap.proto;

import javax.annotation.Nullable;

/**
 * The mapping between a triggering pipeline argument to a triggered pipeline argument.
 */
public class ArgumentMapping {
  @Nullable
  private final String source;
  @Nullable
  private final String target;
  @Nullable
  private final TriggeringPipelineId pipeline;

  public ArgumentMapping(@Nullable String source, @Nullable String target) {
    this(source, target, null);
  }

  public ArgumentMapping(@Nullable String source,
                         @Nullable String target,
                         @Nullable TriggeringPipelineId pipeline) {
    this.source = source;
    this.target = target;
    this.pipeline = pipeline;
  }

  /**
   * @return The name of triggering pipeline argument
   */
  @Nullable
  public String getSource() {
    return source;
  }

  /**
   * @return The name of triggered pipeline argument
   */
  @Nullable
  public String getTarget() {
    return target;
  }

  /**
   * @return The identifiers of properties from the triggering pipeline.
   */
  @Nullable
  public TriggeringPipelineId getTriggeringPipelineId() {
    return pipeline;
  }

  @Override
  public String toString() {
    return "ArgumentMapping{" +
      "source='" + getSource() + '\'' +
      ", target='" + getTarget() + '\'' +
      ", triggeringPipelineId='" + getTriggeringPipelineId() + '\'' +
      '}';
  }
}
