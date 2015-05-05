/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import com.google.gson.JsonElement;

import javax.annotation.Nullable;

/**
 * Represents an adapter returned for /adapters/{adapter-id}.
 */
@Beta
public class AdapterDetail {
  private final String name;
  private final String description;
  private final String template;
  private final ProgramId program;
  private final JsonElement config;

  /**
   * For batch adapters.
   */
  private final ScheduleSpecification schedule;

  /**
   * For realtime adapters.
   */
  private final Integer instances;

  public AdapterDetail(String name, String description, String template, Id.Program program,
                       JsonElement config, ScheduleSpecification schedule, Integer instances) {
    this.name = name;
    this.description = description;
    this.template = template;
    this.instances = instances;
    this.program = new ProgramId(program);
    this.config = config;
    this.schedule = schedule;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getTemplate() {
    return template;
  }

  public ProgramId getProgram() {
    return program;
  }

  public JsonElement getConfig() {
    return config;
  }

  @Nullable
  public ScheduleSpecification getSchedule() {
    return schedule;
  }

  @Nullable
  public Integer getInstances() {
    return instances;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AdapterDetail that = (AdapterDetail) o;

    if (config != null ? !config.equals(that.config) : that.config != null) {
      return false;
    }
    if (description != null
         ? !description.equals(that.description) : that.description != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (program != null
          ? !program.equals(that.program) : that.program != null) {
      return false;
    }
    if (schedule != null
          ? !schedule.equals(that.schedule) : that.schedule != null) {
      return false;
    }
    if (template != null
      ? !template.equals(that.template) : that.template != null) {
      return false;
    }
    if (instances != null
      ? !instances.equals(that.instances) : that.instances != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (description != null ? description.hashCode() : 0);
    result = 31 * result + (template != null ? template.hashCode() : 0);
    result = 31 * result + (program != null ? program.hashCode() : 0);
    result = 31 * result + (config != null ? config.hashCode() : 0);
    result = 31 * result + (schedule != null ? schedule.hashCode() : 0);
    result = 31 * result + (instances != null ? instances.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AdapterDetail{");
    sb.append("name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", template='").append(template).append('\'');
    sb.append(", program=").append(program);
    sb.append(", config=").append(config);
    sb.append(", schedule=").append(schedule);
    sb.append(", instances=").append(instances);
    sb.append('}');
    return sb.toString();
  }
}
