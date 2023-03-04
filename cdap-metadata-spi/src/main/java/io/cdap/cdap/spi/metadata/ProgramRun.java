/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a program run for which lineage is generated.
 */
@Beta
public class ProgramRun {

  private final String runId;
  private final String programId;
  private final String namespace;
  private final String application;
  private final Long startTimeMs;
  private final Long endTimeMs;
  private final String status;
  private final Map<String, String> properties;

  private ProgramRun(String runId, String programId, String namespace, String application,
      @Nullable Long startTimeMs, @Nullable Long endTimeMs, @Nullable String status,
      Map<String, String> properties) {
    if (startTimeMs != null && endTimeMs != null && startTimeMs >= endTimeMs) {
      throw new IllegalArgumentException("ProgramRun endTime has to be greater than startTime.");
    }
    this.runId = runId;
    this.programId = programId;
    this.namespace = namespace;
    this.application = application;
    this.startTimeMs = startTimeMs;
    this.endTimeMs = endTimeMs;
    this.status = status;
    this.properties = properties;
  }

  /**
   * Returns the id of the program run.
   */
  public String getRunId() {
    return runId;
  }

  /**
   * Returns the id of the program which created the run.
   */
  public String getProgramId() {
    return programId;
  }

  /**
   * Returns the namespace in which the program was run.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * Returns the underlying application name for the program.
   */
  public String getApplication() {
    return application;
  }

  /**
   * Returns the startTime of the program run in millis.
   */
  @Nullable
  public Long getStartTimeMs() {
    return startTimeMs;
  }

  /**
   * Returns the endTime of the program run in millis.
   */
  @Nullable
  public Long getEndTimeMs() {
    return endTimeMs;
  }

  /**
   * Returns the status of the program run.
   */
  @Nullable
  public String getStatus() {
    return status;
  }

  /**
   * Returns the properties of the program run.
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String toString() {
    return "ProgramRun{"
        + "id='" + runId + '\''
        + ", programId='" + programId + '\''
        + ", namespace='" + namespace + '\''
        + ", application='" + application + '\''
        + ", startTimeMs=" + startTimeMs
        + ", endTimeMs=" + endTimeMs
        + ", status='" + status + '\''
        + ", properties=" + properties
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProgramRun that = (ProgramRun) o;
    return runId.equals(that.runId)
        && Objects.equals(programId, that.programId)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(application, that.application)
        && Objects.equals(startTimeMs, that.startTimeMs)
        && Objects.equals(endTimeMs, that.endTimeMs)
        && Objects.equals(status, that.status)
        && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(runId, programId, namespace, application, startTimeMs, endTimeMs, status,
        properties);
  }

  public static ProgramRun.Builder builder(String id) {
    return new Builder(id);
  }

  /**
   * A builder to create {@link ProgramRun} instance.
   */
  public static final class Builder {

    private final String id;
    private final Map<String, String> properties;
    private String programId;
    private String namespace;
    private String application;
    private Long startTimeMs;
    private Long endTimeMs;
    private String status;

    private Builder(String id) {
      this.id = id;
      this.properties = new HashMap<>();
    }

    /**
     * Set the ID of the program that created the run.
     */
    public Builder setProgramId(String programId) {
      this.programId = programId;
      return this;
    }

    /**
     * Set the namespace the program run belongs to.
     */
    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Set the underlying application name for the program.
     */
    public Builder setApplication(String application) {
      this.application = application;
      return this;
    }

    /**
     * Set the start time of the program run in millis.
     */
    public Builder setStartTimeMs(@Nullable Long startTimeMs) {
      this.startTimeMs = startTimeMs;
      return this;
    }

    /**
     * Set the end time of the program run in millis.
     */
    public Builder setEndTimeMs(@Nullable Long endTimeMs) {
      this.endTimeMs = endTimeMs;
      return this;
    }

    /**
     * Set the program run status.
     */
    public Builder setStatus(@Nullable String status) {
      this.status = status;
      return this;
    }

    /**
     * Set and replace the plugin properties
     */
    public Builder setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
    }


    /**
     * Adds multiple properties.
     *
     * @param properties map of properties to add.
     * @return this builder
     */
    public Builder addAll(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    /**
     * Adds a property
     *
     * @param key the name of the property
     * @param value the value of the property
     * @return this builder
     */
    public Builder add(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Creates a new instance of {@link ProgramRun}.
     */
    public ProgramRun build() {
      return new ProgramRun(id, programId, namespace, application, startTimeMs, endTimeMs, status,
          properties);
    }
  }
}
