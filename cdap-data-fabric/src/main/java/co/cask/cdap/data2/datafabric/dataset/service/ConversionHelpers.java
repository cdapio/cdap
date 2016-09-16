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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetTypeId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

class ConversionHelpers {

  private static final Gson GSON = new Gson();

  static NamespaceId toNamespaceId(String namespace) throws BadRequestException {
    try {
      return new NamespaceId(namespace);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  static DatasetId toDatasetInstanceId(String namespace, String name) throws BadRequestException {
    try {
      return new DatasetId(namespace, name);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  static DatasetTypeId toDatasetTypeId(String namespace, String typeName) throws BadRequestException {
    try {
      return new DatasetTypeId(namespace, typeName);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  static DatasetTypeId toDatasetTypeId(NamespaceId namespace, String typeName) throws BadRequestException {
    try {
      return new DatasetTypeId(namespace.getNamespace(), typeName);
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  static List<? extends EntityId> strings2ProgramIds(List<String> strings) throws BadRequestException {
    try {
      return Lists.transform(strings, new Function<String, EntityId>() {
        @Nullable
        @Override
        public EntityId apply(@Nullable String input) {
          if (input == null || input.isEmpty()) {
            return null;
          }
          return ProgramId.fromString(input);
        }
      });
    } catch (IllegalArgumentException | NullPointerException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  static Collection<DatasetSpecificationSummary> spec2Summary(Collection<DatasetSpecification> specs) {
    List<DatasetSpecificationSummary> datasetSummaries = Lists.newArrayList();
    for (DatasetSpecification spec : specs) {
      // TODO: (CDAP-3097) handle system datasets specially within a namespace instead of filtering them out
      // by the handler. This filter is only in the list endpoint because the other endpoints are used by
      // HBaseQueueAdmin through DatasetFramework.
      if (QueueConstants.STATE_STORE_NAME.equals(spec.getName())) {
        continue;
      }
      spec = DatasetsUtil.fixOriginalProperties(spec);
      datasetSummaries.add(new DatasetSpecificationSummary(spec.getName(), spec.getType(), spec.getDescription(),
                                                           spec.getOriginalProperties()));
    }
    return datasetSummaries;
  }

  static DatasetInstanceConfiguration getInstanceConfiguration(HttpRequest request) throws BadRequestException {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
    try {
      DatasetInstanceConfiguration config = GSON.fromJson(reader, DatasetInstanceConfiguration.class);
      Preconditions.checkNotNull(config.getTypeName(), "The typeName must be specified.");
      return config;
    } catch (JsonSyntaxException | NullPointerException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  static Map<String, String> getProperties(HttpRequest request) throws BadRequestException {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
    try {
      return GSON.fromJson(reader, new TypeToken<Map<String, String>>() {
      }.getType());
    } catch (JsonSyntaxException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  static String toJson(Map<String, String> properties) {
    return GSON.toJson(properties);
  }
}


