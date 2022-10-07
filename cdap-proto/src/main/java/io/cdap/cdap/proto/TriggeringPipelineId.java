package io.cdap.cdap.proto;

public class TriggeringPipelineId {
  private final String namespace;
  private final String name;

  public TriggeringPipelineId(String namespace, String name) {
    this.namespace = namespace;
    this.name = name;
  }

  /**
   * @return Namespace of the triggering pipeline.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return Names of the triggering pipeline.
   */
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "TriggeringPipelineId{" +
      "namespace='" + getNamespace() + '\'' +
      ", pipelineName='" + getName() + '\'' +
      '}';
  }
}
