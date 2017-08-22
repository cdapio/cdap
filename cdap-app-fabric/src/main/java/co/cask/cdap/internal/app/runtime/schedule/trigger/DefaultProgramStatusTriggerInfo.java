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
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.RunId;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The program status trigger information to be passed to the triggered program.
 */
public class DefaultProgramStatusTriggerInfo extends AbstractTriggerInfo
  implements ProgramStatusTriggerInfo, Externalizable {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .create();

  private String namespace;
  private ApplicationSpecification applicationSpecification;
  private ProgramType programType;
  private String program;
  private RunId runId;
  private ProgramStatus programStatus;
  @Nullable
  private BasicWorkflowToken workflowToken;
  private Map<String, String> runtimeArguments;

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
    this.workflowToken = (BasicWorkflowToken) workflowToken;
    this.runtimeArguments = Collections.unmodifiableMap(new HashMap<>(runtimeArguments));
  }

  public String getNamespace() {
    return namespace;
  }

  public ApplicationSpecification getApplicationSpecification() {
    return applicationSpecification;
  }

  public ProgramType getProgramType() {
    return programType;
  }

  public String getProgram() {
    return program;
  }

  public RunId getRunId() {
    return runId;
  }

  public ProgramStatus getProgramStatus() {
    return programStatus;
  }

  @Nullable
  public WorkflowToken getWorkflowToken() {
    return workflowToken;
  }

  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(namespace);
    byte[] appSpecBytes = GSON.toJson(applicationSpecification).getBytes();
    out.writeInt(appSpecBytes.length);
    out.write(appSpecBytes);
    out.writeObject(programType);
    out.writeUTF(program);
    out.writeUTF(runId.getId());
    out.writeObject(programStatus);
    out.writeObject(workflowToken);
    out.writeObject(runtimeArguments);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    namespace = in.readUTF();
    int appSpecBytesLength = in.readInt();
    byte[] appSpecBytes = new byte[appSpecBytesLength];
    in.read(appSpecBytes);
    applicationSpecification = GSON.fromJson(new String(appSpecBytes, StandardCharsets.UTF_8),
                                             ApplicationSpecification.class);
    programType = (ProgramType) in.readObject();
    program = in.readUTF();
    runId = RunIds.fromString(in.readUTF());
    programStatus = (ProgramStatus) in.readObject();
    workflowToken = (BasicWorkflowToken) in.readObject();
    runtimeArguments = (Map<String, String>) in.readObject();
  }
}
