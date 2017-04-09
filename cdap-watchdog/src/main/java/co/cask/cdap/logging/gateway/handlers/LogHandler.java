/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.logging.gateway.handlers;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.gateway.handlers.store.ProgramStore;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * v3 {@link HttpHandler} to handle /logs requests
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
public class LogHandler extends AbstractLogHandler {

  private final ProgramStore programStore;

  @Inject
  public LogHandler(LogReader logReader, CConfiguration cConfig, ProgramStore programStore) {
    super(logReader, cConfig);
    this.programStore = programStore;
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs")
  public void getLogs(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                      @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                      @PathParam("program-id") String programId,
                      @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                      @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                             ProgramType.valueOfCategoryName(programType));
    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, null, "text",
              ImmutableList.<String>of());
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs")
  public void getRunIdLogs(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                           @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                           @PathParam("program-id") String programId, @PathParam("run-id") String runId,
                           @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                           @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                           @QueryParam("escape") @DefaultValue("true") boolean escape,
                           @QueryParam("filter") @DefaultValue("") String filterStr,
                           @QueryParam("format") @DefaultValue("text") String format,
                           @QueryParam("suppress") List<String> suppress) throws NotFoundException {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    RunRecordMeta runRecord = getRunRecordMeta(namespaceId, appId, type, programId, runId);
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId, type,
                                                                                    runId, runRecord.getSystemArgs());

    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, runRecord, format,
              suppress);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs/next")
  public void next(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId,
                                             programId, ProgramType.valueOfCategoryName(programType));
    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, "text",
           ImmutableList.<String>of());
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs/next")
  public void runIdNext(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                        @PathParam("program-id") String programId, @PathParam("run-id") String runId,
                        @QueryParam("max") @DefaultValue("50") int maxEvents,
                        @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                        @QueryParam("escape") @DefaultValue("true") boolean escape,
                        @QueryParam("filter") @DefaultValue("") String filterStr,
                        @QueryParam("format") @DefaultValue("text") String format,
                        @QueryParam("suppress") List<String> suppress) throws NotFoundException {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    RunRecordMeta runRecord = getRunRecordMeta(namespaceId, appId, type, programId, runId);
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId, type,
                                                                                    runId, runRecord.getSystemArgs());

    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord, format, suppress);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/logs/prev")
  public void prev(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                   @PathParam("program-id") String programId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                   @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                   @QueryParam("escape") @DefaultValue("true") boolean escape,
                   @QueryParam("filter") @DefaultValue("") String filterStr) {
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContext(namespaceId, appId, programId,
                                             ProgramType.valueOfCategoryName(programType));
    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, "text",
           ImmutableList.<String>of());
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs/prev")
  public void runIdPrev(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("app-id") String appId, @PathParam("program-type") String programType,
                        @PathParam("program-id") String programId, @PathParam("run-id") String runId,
                        @QueryParam("max") @DefaultValue("50") int maxEvents,
                        @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                        @QueryParam("escape") @DefaultValue("true") boolean escape,
                        @QueryParam("filter") @DefaultValue("") String filterStr,
                        @QueryParam("format") @DefaultValue("text") String format,
                        @QueryParam("suppress") List<String> suppress) throws NotFoundException {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    RunRecordMeta runRecord = getRunRecordMeta(namespaceId, appId, type, programId, runId);
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(namespaceId, appId, programId, type,
                                                                                    runId, runRecord.getSystemArgs());

    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord, format, suppress);
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs")
  public void sysList(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId,
                      @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                      @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr,
                      @QueryParam("format") @DefaultValue("text") String format,
                      @QueryParam("suppress") List<String> suppress) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Id.Namespace.SYSTEM.getId(), componentId,
                                                                           serviceId);
    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, null, format, suppress);
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/next")
  public void sysNext(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr,
                      @QueryParam("format") @DefaultValue("text") String format,
                      @QueryParam("suppress") List<String> suppress) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Id.Namespace.SYSTEM.getId(), componentId,
                                                                           serviceId);
    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, format, suppress);
  }

  @GET
  @Path("/system/{component-id}/{service-id}/logs/prev")
  public void sysPrev(HttpRequest request, HttpResponder responder, @PathParam("component-id") String componentId,
                      @PathParam("service-id") String serviceId, @QueryParam("max") @DefaultValue("50") int maxEvents,
                      @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                      @QueryParam("escape") @DefaultValue("true") boolean escape,
                      @QueryParam("filter") @DefaultValue("") String filterStr,
                      @QueryParam("format") @DefaultValue("text") String format,
                      @QueryParam("suppress") List<String> suppress) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(Id.Namespace.SYSTEM.getId(), componentId,
                                                                           serviceId);
    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, null, format, suppress);
  }

  private RunRecordMeta getRunRecordMeta(String namespace, String app, ProgramType programType,
                                         String programName, String run) throws NotFoundException {
    ProgramRunId programRunId = new ProgramRunId(namespace, app, programType, programName, run);
    RunRecordMeta runRecord = programStore.getRun(programRunId.getParent(), programRunId.getRun());
    if (runRecord == null) {
      throw new NotFoundException(programRunId);
    }
    return runRecord;
  }
}
