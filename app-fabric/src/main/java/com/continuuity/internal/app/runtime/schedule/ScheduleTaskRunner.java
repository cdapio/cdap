package com.continuuity.internal.app.runtime.schedule;

import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.RunIdentifier;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Task runner that runs a schedule.
 */
public final class ScheduleTaskRunner {


  private static final Logger LOG = LoggerFactory.getLogger(ScheduleTaskRunner.class);

  private final StoreFactory storeFactory;
  private final ProgramRuntimeService runtimeService;
  private final Store store;

  public ScheduleTaskRunner(StoreFactory storeFactory, ProgramRuntimeService runtimeService)
    throws AppFabricServiceException {
    this.storeFactory = storeFactory;
    this.runtimeService = runtimeService;
    this.store = storeFactory.create();
  }


  public  synchronized  RunIdentifier run(String accountId, String applicationId, String flowId,
                                          ProgramOptions options) {

    FlowIdentifier id = new FlowIdentifier(accountId, applicationId, flowId, 1);
    ProgramRuntimeService.RuntimeInfo existingRuntimeInfo = findRuntimeInfo(id);
    Preconditions.checkArgument(existingRuntimeInfo == null, UserMessages.getMessage(UserErrors.ALREADY_RUNNING));
    Id.Program programId = Id.Program.from(accountId, applicationId, flowId);

    Program program;
    try {
      program =  store.loadProgram(programId, Type.WORKFLOW);
    } catch (Throwable th) {
      throw Throwables.propagate(th);
    }

    BasicArguments userArguments = new BasicArguments();

    ProgramRuntimeService.RuntimeInfo runtimeInfo =
      runtimeService.run(program, new SimpleProgramOptions(id.getFlowId(),
                                                           new BasicArguments(),
                                                           userArguments));
    store.setStart(programId, runtimeInfo.getController().getRunId().getId(),
                   TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
    return new RunIdentifier(runtimeInfo.getController().getRunId().toString());

  }

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(FlowIdentifier identifier) {
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = null;
    switch (identifier.getType()) {
      case FLOW:
        runtimeInfos = runtimeService.list(Type.FLOW).values();
        break;
      case PROCEDURE:
        runtimeInfos = runtimeService.list(Type.PROCEDURE).values();
        break;
      case MAPREDUCE:
        runtimeInfos = runtimeService.list(Type.MAPREDUCE).values();
        break;
    }
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               identifier.getAccountId(), identifier.getFlowId());

    Id.Program programId = Id.Program.from(identifier.getAccountId(),
                                           identifier.getApplicationId(),
                                           identifier.getFlowId());

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }
}
