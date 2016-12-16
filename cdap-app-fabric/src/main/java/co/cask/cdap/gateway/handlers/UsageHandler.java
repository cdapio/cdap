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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.EntityIdCompatible;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link co.cask.http.HttpHandler} for handling REST calls to the usage registry.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class UsageHandler extends AbstractHttpHandler {

  private final UsageRegistry registry;

  @Inject
  public UsageHandler(UsageRegistry registry) {
    this.registry = registry;
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/datasets")
  public void getAppDatasetUsage(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId) {
    final ApplicationId id = new ApplicationId(namespaceId, appId);
    Set<DatasetId> ids = registry.getDatasets(id);
    responder.sendJson(HttpResponseStatus.OK,
                       BackwardCompatibility.IdDatasetInstance.transform(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/streams")
  public void getAppStreamUsage(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) {
    final ApplicationId id = new ApplicationId(namespaceId, appId);
    Set<StreamId> ids = registry.getStreams(id);
    responder.sendJson(HttpResponseStatus.OK,
                       BackwardCompatibility.IdStream.transform(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/datasets")
  public void getProgramDatasetUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("program-type") String programType,
                                     @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final ProgramId id = new ProgramId(namespaceId, appId, type, programId);
    Set<DatasetId> ids = registry.getDatasets(id);
    responder.sendJson(HttpResponseStatus.OK,
                       BackwardCompatibility.IdDatasetInstance.transform(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/streams")
  public void getProgramStreamUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final ProgramId id = new ProgramId(namespaceId, appId, type, programId);
    Set<StreamId> ids = registry.getStreams(id);
    responder.sendJson(HttpResponseStatus.OK,
                       BackwardCompatibility.IdStream.transform(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/programs")
  public void getStreamProgramUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("stream-id") String streamId) {
    final StreamId id = new StreamId(namespaceId, streamId);
    Set<ProgramId> ids = registry.getPrograms(id);
    responder.sendJson(HttpResponseStatus.OK,
                       BackwardCompatibility.IdProgram.transform(ids));
  }

  @GET
  @Path("/namespaces/{namespace-id}/data/datasets/{dataset-id}/programs")
  public void getDatasetAppUsage(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) {
    final DatasetId id = new DatasetId(namespaceId, datasetId);
    Set<ProgramId> ids = registry.getPrograms(id);
    responder.sendJson(HttpResponseStatus.OK,
                       BackwardCompatibility.IdProgram.transform(ids));
  }

  /**
   * Class to wrap stuff needed to make {@link UsageHandler} backward compatible. See CDAP-7316
   * Just a simple POJO to support backward compatibility for {@link UsageHandler} See: CDAP-7316
   * This class allows {@link UsageHandler} to return results in deprecated {@link Id} serialized form.
   */
  public static final class BackwardCompatibility {

    /**
     * Support Compatibility with {@link Id.Namespace} for {@link NamespaceId}
     */
    static final class IdNamespace implements EntityIdCompatible {
      private final String id;

      IdNamespace(NamespaceId id) {
        this.id = id.getNamespace();
      }

      @Override
      public EntityId toEntityId() {
        return new NamespaceId(id);
      }
    }

    /**
     * Support Compatibility with {@link Id.DatasetInstance} for {@link DatasetId}
     */
    public static final class IdDatasetInstance implements EntityIdCompatible {
      private final IdNamespace namespace;
      private final String instanceId;

      IdDatasetInstance(DatasetId datasetId) {
        this.namespace = new IdNamespace(datasetId.getNamespaceId());
        this.instanceId = datasetId.getEntityName();
      }

      static Set<IdDatasetInstance> transform(Set<DatasetId> datasetIds) {
        Set<IdDatasetInstance> idCompatibleEntityIds = new HashSet<>();
        for (DatasetId datasetId : datasetIds) {
          idCompatibleEntityIds.add(new IdDatasetInstance(datasetId));
        }
        return idCompatibleEntityIds;
      }

      @Override
      public EntityId toEntityId() {
        return new DatasetId(namespace.id, instanceId);
      }
    }

    /**
     * Support Compatibility with {@link Id.Stream} for {@link StreamId}
     */
    public static final class IdStream implements EntityIdCompatible {
      private final IdNamespace namespace;
      private final String streamName;

      IdStream(StreamId streamId) {
        this.namespace = new IdNamespace(streamId.getNamespaceId());
        this.streamName = streamId.getEntityName();
      }

      static Set<IdStream> transform(Set<StreamId> streamIds) {
        Set<IdStream> idCompatibleEntityIds = new HashSet<>();
        for (StreamId streamId : streamIds) {
          idCompatibleEntityIds.add(new IdStream(streamId));
        }
        return idCompatibleEntityIds;
      }

      @Override
      public EntityId toEntityId() {
        return new StreamId(namespace.id, streamName);
      }
    }

    /**
     * Support Compatibility with {@link Id.Application} for {@link ApplicationId}
     */
    static final class IdApplication implements EntityIdCompatible {
      private final IdNamespace namespace;
      private final String applicationId;

      IdApplication(ApplicationId applicationId) {
        this.namespace = new IdNamespace(applicationId.getNamespaceId());
        this.applicationId = applicationId.getEntityName();
      }

      @Override
      public EntityId toEntityId() {
        return new ApplicationId(namespace.id, applicationId);
      }
    }

    /**
     * Support Compatibility with {@link Id.Program} for {@link ProgramId}
     */
    public static final class IdProgram implements EntityIdCompatible {
      private final IdApplication application;
      private final ProgramType type;
      private final String id;

      IdProgram(ProgramId programId) {
        this.application = new IdApplication(programId.getParent());
        this.type = programId.getType();
        this.id = programId.getEntityName();
      }

      static Set<IdProgram> transform(Set<ProgramId> programIds) {
        Set<IdProgram> idCompatibleEntityIds = new HashSet<>();
        for (ProgramId programId : programIds) {
          idCompatibleEntityIds.add(new IdProgram(programId));
        }
        return idCompatibleEntityIds;
      }

      @Override
      public EntityId toEntityId() {
        return new ProgramId(application.namespace.id, application.applicationId, type, id);
      }
    }
  }
}
