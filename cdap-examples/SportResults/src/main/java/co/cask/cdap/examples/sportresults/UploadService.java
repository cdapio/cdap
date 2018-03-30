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

package co.cask.cdap.examples.sportresults;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.PartitionConsumerResult;
import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Preconditions;
import org.apache.tephra.TransactionFailureException;

import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class UploadService extends AbstractService {

  @Override
  protected void configure() {
    setName("PartitionedFileSetService");
    setDescription("A service for managing partitions of PartitionedFileSets.");
    addHandler(new PartitionHandler());
  }

  /**
   *
   */
  public static class PartitionHandler extends AbstractHttpServiceHandler {
      private static final Predicate<PartitionDetail> ALWAYS_TRUE = new Predicate<PartitionDetail>() {
          @Override
          public boolean apply(PartitionDetail partitionDetail) {
              return true;
          }
      };

      @Path("dataset/{datasetName}")
      @GET
      public void history(HttpServiceRequest request, HttpServiceResponder responder,
              @PathParam("datasetName") String datasetName) {
          Dataset dataset = getContext().getDataset(datasetName);
          if (!(dataset instanceof PartitionedFileSet)) {
              throw new IllegalArgumentException();
          }
          PartitionedFileSet pfs = (PartitionedFileSet) dataset;
          PartitionConsumerResult result =
                  pfs.consumePartitions(PartitionConsumerState.FROM_BEGINNING, 100, ALWAYS_TRUE);
          responder.sendJson(result.getPartitions());
      }

    @DELETE
    @Path("dataset/{datasetName}")
    @TransactionPolicy(TransactionControl.EXPLICIT)
    public void deletePartition(HttpServiceRequest request, HttpServiceResponder responder,
            @PathParam("datasetName") final String datasetName) throws TransactionFailureException {

        getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
                Dataset dataset = context.getDataset(datasetName);
                if (!(dataset instanceof PartitionedFileSet)) {
                    throw new IllegalArgumentException();
                }
                PartitionedFileSet pfs = (PartitionedFileSet) dataset;
                Partitioning partitioning = pfs.getPartitioning();

                PartitionKey.Builder keyBuilder = PartitionKey.builder(partitioning);
                for (Map.Entry<String, Partitioning.FieldType> fieldTypeEntry : partitioning.getFields().entrySet()) {
                    String headerValue = request.getHeader(fieldTypeEntry.getKey());
                    Preconditions.checkArgument(headerValue != null);
                    keyBuilder.addField(fieldTypeEntry.getKey(), fieldTypeEntry.getValue().parse(headerValue));
                }

                PartitionKey partitionKey = keyBuilder.build();
                pfs.dropPartition(partitionKey);
            }
        });
        responder.sendStatus(200);
    }
  }
}
