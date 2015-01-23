
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetCacheKey;
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
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * The MapReduce program runtime context for use in the {@link AbstractMapReduce#beforeSubmit(MapReduceContext)} and
 * {@link AbstractMapReduce#onFinish(boolean, MapReduceContext)} methods.
 *
 * <p>
 * The runtime context allows the retrieval of datasets that were not specified in the application specification,
 * letting users access datasets at runtime that aren't known about at compile time.
 * </p>
 */
public class DynamicMapReduceContext extends DynamicDatasetContext implements MapReduceContext {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicMapReduceContext.class);
  private final BasicMapReduceContext mapReduceContext;
  private final LoadingCache<Long, Map<DatasetCacheKey, Dataset>> datasetsCache;

  public DynamicMapReduceContext(BasicMapReduceContext mapReduceContext,
                                 DatasetFramework datasetFramework,
                                 TransactionContext transactionContext,
                                 CConfiguration cConf) {
    super(transactionContext,
          new NamespacedDatasetFramework(datasetFramework, new DefaultDatasetNamespace(cConf, Namespace.USER)),
          mapReduceContext.getProgram().getClassLoader(),
          null,
          mapReduceContext.getRuntimeArguments());
    this.mapReduceContext = mapReduceContext;
    this.datasetsCache = CacheBuilder.newBuilder()
      .removalListener(new RemovalListener<Long, Map<DatasetCacheKey, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public void onRemoval(RemovalNotification<Long, Map<DatasetCacheKey, Dataset>> notification) {
          if (notification.getValue() != null) {
            for (Map.Entry<DatasetCacheKey, Dataset> entry : notification.getValue().entrySet()) {
              try {
                entry.getValue().close();
              } catch (IOException e) {
                LOG.error("Error closing dataset: {}", entry.getKey(), e);
              }
            }
          }
        }
      })
      .build(new CacheLoader<Long, Map<DatasetCacheKey, Dataset>>() {
        @Override
        @ParametersAreNonnullByDefault
        public Map<DatasetCacheKey, Dataset> load(Long key) throws Exception {
          return Maps.newHashMap();
        }
      });
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
  public void setMapperResources(Resources resources) {
    mapReduceContext.setMapperResources(resources);
  }

  @Override
  public void setReducerResources(Resources resources) {
    mapReduceContext.setReducerResources(resources);
  }

  @Nullable
  @Override
  protected LoadingCache<Long, Map<DatasetCacheKey, Dataset>> getDatasetsCache() {
    return datasetsCache;
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
