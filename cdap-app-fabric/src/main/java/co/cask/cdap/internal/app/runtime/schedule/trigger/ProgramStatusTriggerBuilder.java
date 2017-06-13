package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.internal.schedule.trigger.TriggerBuilder;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;

import javax.annotation.Nullable;

/**
 * A Trigger that schedules a ProgramSchedule, based upon a particular cron expression.
 */
public class ProgramStatusTriggerBuilder implements TriggerBuilder {
  private String programNamespace;
  private String programApplication;
  private String programApplicationVersion;
  private ProgramType programType;
  private String programName;
  private ProgramStatus programStatus;

  public ProgramStatusTriggerBuilder(@Nullable String namespace, @Nullable String application,
                                     @Nullable String applicationVersion, String programType, String programName,
                                     ProgramStatus programStatus) {
    this.programNamespace = namespace;
    this.programApplication = application;
    this.programApplicationVersion = applicationVersion;
    this.programType = ProgramType.valueOf(programType);
    this.programName = programName;
    this.programStatus = programStatus;
  }

  @Override
  public ProgramStatusTrigger build(String namespace, String application, String applicationVersion) {
    if (programNamespace == null) {
      programNamespace  = namespace;
    }
    if (programApplication  == null) {
      programApplication = application;
    }
    if (programApplicationVersion == null) {
      programApplicationVersion = applicationVersion;
    }

    return new ProgramStatusTrigger(
            new ApplicationId(programNamespace, programApplication, programApplicationVersion)
                    .program(programType, programName),
            programStatus
    );
  }
}
