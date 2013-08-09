/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.AbstractProgramRuntimeService;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.service.SimpleRuntimeInfo;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public final class DistributedProgramRuntimeService extends AbstractProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRuntimeService.class);

  // Pattern to split a Weave App name into [type].[accountId].[appName].[programName]
  private static final Pattern APP_NAME_PATTERN = Pattern.compile("^(\\S+)\\.(\\S+)\\.(\\S+)\\.(\\S+)$");

  private final WeaveRunner weaveRunner;

  @Inject
  DistributedProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, WeaveRunner weaveRunner) {
    super(programRunnerFactory);
    this.weaveRunner = weaveRunner;
  }

  @Override
  public synchronized RuntimeInfo lookup(final RunId runId) {
    RuntimeInfo runtimeInfo = super.lookup(runId);
    if (runtimeInfo != null) {
      return runtimeInfo;
    }

    // Lookup all live applications and look for the one that matches runId
    String appName = null;
    WeaveController controller = null;
    for (WeaveRunner.LiveInfo liveInfo : weaveRunner.lookupLive()) {
      for (WeaveController c : liveInfo.getControllers()) {
        if (c.getRunId().equals(runId)) {
          appName = liveInfo.getApplicationName();
          controller = c;
          break;
        }
      }
      if (controller != null) {
        break;
      }
    }

    if (controller == null) {
      LOG.info("No running instance found for RunId {}", runId);
      // TODO (ENG-2623): How about mapreduce job?
      return null;
    }

    Matcher matcher = APP_NAME_PATTERN.matcher(appName);
    if (!matcher.matches()) {
      LOG.warn("Unrecognized application name pattern {}", appName);
      return null;
    }

    Type type = getType(matcher.group(1));
    if (type == null) {
      LOG.warn("Unrecognized program type {}", appName);
      return null;
    }
    Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));

    runtimeInfo = createRuntimeInfo(type, programId, controller);
    updateRuntimeInfo(type, runId, runtimeInfo);
    return runtimeInfo;
  }

  @Override
  public synchronized Map<RunId, RuntimeInfo> list(Type type) {
    Map<RunId, RuntimeInfo> result = Maps.newHashMap();
    result.putAll(super.list(type));

    // Goes through all live application, filter out the one that match the given type.
    for (WeaveRunner.LiveInfo liveInfo : weaveRunner.lookupLive()) {
      String appName = liveInfo.getApplicationName();
      Matcher matcher = APP_NAME_PATTERN.matcher(appName);
      if (!matcher.matches()) {
        continue;
      }
      Type appType = getType(matcher.group(1));
      if (appType != type) {
        continue;
      }

      for (WeaveController controller : liveInfo.getControllers()) {
        RunId runId = controller.getRunId();
        if (result.containsKey(runId)) {
          continue;
        }
        Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));
        RuntimeInfo runtimeInfo = createRuntimeInfo(type, programId, controller);
        result.put(runId, runtimeInfo);
        updateRuntimeInfo(type, runId, runtimeInfo);
      }
    }
    return ImmutableMap.copyOf(result);
  }

  private RuntimeInfo createRuntimeInfo(Type type, Id.Program programId, WeaveController controller) {
    ProgramController programController = createController(type, programId.getId(), controller);
    return programController == null ? null : new SimpleRuntimeInfo(programController, type, programId);
  }

  private ProgramController createController(Type type, String programId, WeaveController controller) {
    AbstractWeaveProgramController programController = null;
    switch (type) {
      case FLOW:
        programController = new FlowWeaveProgramController(programId, controller);
        break;
      case PROCEDURE:
        programController = new ProcedureWeaveProgramController(programId, controller);
        break;
      case MAPREDUCE:
        programController = new MapReduceWeaveProgramController(programId, controller);
        break;
    }
    return programController == null ? null : programController.startListen();
  }

  private Type getType(String typeName) {
    try {
      return Type.valueOf(typeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
