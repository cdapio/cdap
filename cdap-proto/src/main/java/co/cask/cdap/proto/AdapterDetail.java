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

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.internal.Objects;
import com.google.gson.JsonElement;

/**
 * Represents an adapter returned for /adapters/{adapter-id}.
 */
public class AdapterDetail {
  private final String name;
  private final String description;
  private final String template;
  private final ProgramId program;
  private final JsonElement config;
  private final ScheduleSpecification schedule;

  public AdapterDetail(String name, String description, String template, Id.Program program,
                       JsonElement config, ScheduleSpecification schedule) {
    this.name = name;
    this.description = description;
    this.template = template;
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

  public ScheduleSpecification getSchedule() {
    return schedule;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, description, template, program, config, schedule);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final AdapterDetail other = (AdapterDetail) obj;
    return Objects.equal(this.name, other.name) && Objects.equal(this.description, other.description) &&
      Objects.equal(this.template, other.template) && Objects.equal(this.program, other.program) &&
      Objects.equal(this.config, other.config) && Objects.equal(this.schedule, other.schedule);
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
    sb.append('}');
    return sb.toString();
  }
}
