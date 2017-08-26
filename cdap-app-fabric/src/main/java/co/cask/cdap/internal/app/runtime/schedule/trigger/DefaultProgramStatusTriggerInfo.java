/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.api.schedule.ProgramStatusTriggerInfo;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.RunId;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The program status trigger information to be passed to the triggered program.
 */
public class DefaultProgramStatusTriggerInfo extends AbstractTriggerInfo
  implements ProgramStatusTriggerInfo, Externalizable {

  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private String namespace;
  private ApplicationSpecification applicationSpecification;
  private ProgramType programType;
  private String program;
  private RunId runId;
  private ProgramStatus programStatus;
  @Nullable
  private WorkflowToken workflowToken;
  private Map<String, String> runtimeArguments;

  /**
   * Constructor for {@link Externalizable} to use only. Don't call it directly.
   */
  public DefaultProgramStatusTriggerInfo() {
    super(Type.PROGRAM_STATUS);
  }

  public DefaultProgramStatusTriggerInfo(String namespace, ApplicationSpecification applicationSpecification,
                                         ProgramType programType, String program,
                                         RunId runId, ProgramStatus programStatus,
                                         @Nullable WorkflowToken workflowToken,
                                         Map<String, String> runtimeArguments) {
    super(Type.PROGRAM_STATUS);
    this.namespace = namespace;
    this.applicationSpecification = applicationSpecification;
    this.programType = programType;
    this.program = program;
    this.runId = runId;
    this.programStatus = programStatus;
    this.workflowToken = workflowToken;
    this.runtimeArguments = Collections.unmodifiableMap(new HashMap<>(runtimeArguments));
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return applicationSpecification;
  }

  @Override
  public ProgramType getProgramType() {
    return programType;
  }

  @Override
  public String getProgram() {
    return program;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public ProgramStatus getProgramStatus() {
    return programStatus;
  }

  @Nullable
  @Override
  public WorkflowToken getWorkflowToken() {
    return workflowToken;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(namespace);

    // Can't use writeUTF for app spec because it could be larger than 64K
    out.writeObject(GSON.toJson(applicationSpecification));

    out.writeObject(programType);
    out.writeUTF(program);
    out.writeUTF(runId.getId());
    out.writeObject(programStatus);
    out.writeObject(workflowToken);

    // Write out the map size and the entries
    out.writeInt(runtimeArguments.size());
    for (Map.Entry<String, String> entry : runtimeArguments.entrySet()) {
      out.writeObject(entry.getKey());
      out.writeObject(entry.getValue());
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    namespace = in.readUTF();
    applicationSpecification = GSON.fromJson((String) in.readObject(), ApplicationSpecification.class);

    programType = (ProgramType) in.readObject();
    program = in.readUTF();
    runId = RunIds.fromString(in.readUTF());
    programStatus = (ProgramStatus) in.readObject();
    workflowToken = (WorkflowToken) in.readObject();

    Map<String, String> args = new HashMap<>();
    int argsSize = in.readInt();
    for (int i = 0; i < argsSize; i++) {
      args.put((String) in.readObject(), (String) in.readObject());
    }

    runtimeArguments = Collections.unmodifiableMap(args);
  }
}
