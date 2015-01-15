
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetContext;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.tephra.TransactionContext;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Mapreduce job runtime context for use in the {@link AbstractMapReduce#beforeSubmit(MapReduceContext)} and
 * {@link AbstractMapReduce#onFinish(boolean, MapReduceContext)} methods, which allow getting datasets
 * that were not specified in the application specification. This allows users to get datasets at runtime
 * that they didn't know about at compile time.
 */
public class BasicMapReduceContextWithTX implements MapReduceContext {
  private static final Logger LOG = LoggerFactory.getLogger(BasicMapReduceContextWithTX.class);
  private final BasicMapReduceContext mapReduceContext;
  // this is used for datasets that we can't get through the BasicMapReduceContext
  private final DynamicDatasetContext dynamicDatasetContext;
  private final LoadingCache<Long, Map<String, Dataset>> datasetsCache;

  public BasicMapReduceContextWithTX(BasicMapReduceContext mapReduceContext,
                                     DatasetFramework datasetFramework,
                                     TransactionContext transactionContext,
                                     CConfiguration cConf) {
    this.mapReduceContext = mapReduceContext;
    // TODO: this should already be namespaced when passed in.
    DatasetFramework namespacedDatasetFramework = new NamespacedDatasetFramework(
      datasetFramework, new DefaultDatasetNamespace(cConf, Namespace.USER));
    // A cache of datasets by threadId. Repeated requests for a dataset from the same thread returns the same
    // instance, thus avoiding the overhead of creating a new instance for every request.
    this.datasetsCache = CacheBuilder.newBuilder()
      .removalListener(new RemovalListener<Long, Map<String, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<Long, Map<String, Dataset>> notification) {
          if (notification.getValue() != null) {
            for (Map.Entry<String, Dataset> entry : notification.getValue().entrySet()) {
              try {
                entry.getValue().close();
              } catch (IOException e) {
                LOG.error("Error closing dataset: {}", entry.getKey(), e);
              }
            }
          }
        }
      })
      .build(new CacheLoader<Long, Map<String, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public Map<String, Dataset> load(Long key) throws Exception {
          return Maps.newHashMap();
        }
      });
    this.dynamicDatasetContext = new DynamicDatasetContext(
      transactionContext,
      namespacedDatasetFramework,
      mapReduceContext.getProgram().getClassLoader(),
      null) {
      @Override
      protected LoadingCache<Long, Map<String, Dataset>> getDatasetsCache() {
        return null;
      }
    };
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return mapReduceContext.getSpecification();
  }

  @Override
  public long getLogicalStartTime() {
    return mapReduceContext.getLogicalStartTime();
  }

  @Override
  public <T> T getHadoopJob() {
    return mapReduceContext.getHadoopJob();
  }

  @Override
  public void setInput(String datasetName) {
    mapReduceContext.setInput(datasetName);
  }

  @Override
  public void setInput(String datasetName, List<Split> splits) {
    mapReduceContext.setInput(datasetName, splits);
  }

  @Override
  public void setInput(String datasetName, Dataset dataset) {
    mapReduceContext.setInput(datasetName, dataset);
  }

  @Override
  public void setOutput(String datasetName) {
    mapReduceContext.setOutput(datasetName);
  }

  @Override
  public void setOutput(String datasetName, Dataset dataset) {
    mapReduceContext.setOutput(datasetName, dataset);
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    try {
      return mapReduceContext.getDataset(name);
    } catch (DatasetInstantiationException e) {
      return dynamicDatasetContext.getDataset(name);
    }
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    try {
      return mapReduceContext.getDataset(name, arguments);
    } catch (DatasetInstantiationException e) {
      return dynamicDatasetContext.getDataset(name, arguments);
    }
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return mapReduceContext.getRuntimeArguments();
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return mapReduceContext.getServiceURL(applicationId, serviceId);
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return mapReduceContext.getServiceURL(serviceId);
  }

  public void close() {
    datasetsCache.invalidateAll();
    datasetsCache.cleanUp();
  }
}
