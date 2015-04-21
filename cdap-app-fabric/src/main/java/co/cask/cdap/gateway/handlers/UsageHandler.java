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
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.data2.registry.UsageDataset;
import co.cask.cdap.data2.registry.UsageDatasets;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * The {@link co.cask.http.HttpHandler} for handling REST calls to the usage registry.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class UsageHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UsageHandler.class);
  private final Transactional<UsageDatasetIterable, UsageDataset> txnl;

  @Inject
  public UsageHandler(final DatasetFramework datasetFramework, TransactionExecutorFactory txExecutorFactory) {
    txnl = Transactional.of(txExecutorFactory, new Supplier<UsageDatasetIterable>() {
      @Override
      public UsageDatasetIterable get() {
        try {
          return new UsageDatasetIterable(UsageDatasets.get(datasetFramework));
        } catch (Exception e) {
          LOG.error("Failed to access usage table", e);
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/datasets")
  public void getAppDatasetUsage(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("app-id") String appId) {
    final Id.Application id = Id.Application.from(namespaceId, appId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getDatasets(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/streams")
  public void getAppStreamUsage(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("app-id") String appId) {
    final Id.Application id = Id.Application.from(namespaceId, appId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getStreams(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/datasets")
  public void getProgramDatasetUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("app-id") String appId,
                                     @PathParam("program-type") String programType,
                                     @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final Id.Program id = Id.Program.from(namespaceId, appId, type, programId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getDatasets(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/streams")
  public void getProgramStreamUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("app-id") String appId,
                                    @PathParam("program-type") String programType,
                                    @PathParam("program-id") String programId) {
    ProgramType type = ProgramType.valueOfCategoryName(programType);
    final Id.Program id = Id.Program.from(namespaceId, appId, type, programId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getStreams(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/adapters/{adapter-id}/datasets")
  public void getAdapterDatasetUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("adapter-id") String adapterId) {
    final Id.Adapter id = Id.Adapter.from(namespaceId, adapterId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getDatasets(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/adapters/{adapter-id}/streams")
  public void getAdapterStreamUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("adapter-id") String adapterId) {
    final Id.Adapter id = Id.Adapter.from(namespaceId, adapterId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getStreams(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/programs")
  public void getStreamProgramUsage(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId,
                                @PathParam("stream-id") String streamId) {
    final Id.Stream id = Id.Stream.from(namespaceId, streamId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getPrograms(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/streams/{stream-id}/adapters")
  public void getStreamAdapterUsage(HttpRequest request, HttpResponder responder,
                                    @PathParam("namespace-id") String namespaceId,
                                    @PathParam("stream-id") String streamId) {
    final Id.Stream id = Id.Stream.from(namespaceId, streamId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
      @Override
      public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
        return input.getUsageDataset().getAdapters(id);
      }
    });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/data/datasets/{dataset-id}/programs")
  public void getDatasetAppUsage(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("dataset-id") String datasetId) {
    final Id.DatasetInstance id = Id.DatasetInstance.from(namespaceId, datasetId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getPrograms(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @GET
  @Path("/namespaces/{namespace-id}/data/datasets/{dataset-id}/adapters")
  public void getDatasetAdapterUsage(HttpRequest request, HttpResponder responder,
                                     @PathParam("namespace-id") String namespaceId,
                                     @PathParam("dataset-id") String datasetId) {
    final Id.DatasetInstance id = Id.DatasetInstance.from(namespaceId, datasetId);
    Set<? extends Id> ids = executeUsageDatasetOp(
      new TransactionExecutor.Function<UsageDatasetIterable, Set<? extends Id>>() {
        @Override
        public Set<? extends Id> apply(UsageDatasetIterable input) throws Exception {
          return input.getUsageDataset().getAdapters(id);
        }
      });
    responder.sendJson(HttpResponseStatus.OK, ids);
  }

  @VisibleForTesting
  public <R> R executeUsageDatasetOp(TransactionExecutor.Function<UsageDatasetIterable, R> func) {
    return txnl.executeUnchecked(func);
  }

  /**
   * For passing {@link UsageDataset} to {@link Transactional#of}.
   */
  public static final class UsageDatasetIterable implements Iterable<UsageDataset> {
    private final UsageDataset usageDataset;

    private UsageDatasetIterable(UsageDataset usageDataset) {
      this.usageDataset = usageDataset;
    }

    public UsageDataset getUsageDataset() {
      return usageDataset;
    }

    @Override
    public Iterator<UsageDataset> iterator() {
      return Iterators.singletonIterator(usageDataset);
    }
  }
}
