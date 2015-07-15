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

package co.cask.cdap.data2.util.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * Builder for creating {@link Get}. This builder should be used for cross HBase versions compatibility.
 * All methods on this class are just delegating to calls to {@link Get} object.
 */
public interface GetBuilder {

  GetBuilder addFamily(byte [] family);

  GetBuilder addColumn(byte [] family, byte [] qualifier);

  GetBuilder setTimeRange(long minStamp, long maxStamp) throws IOException;

  GetBuilder setTimeStamp(long timestamp) throws IOException;

  GetBuilder setMaxVersions();

  GetBuilder setMaxVersions(int maxVersions) throws IOException;

  GetBuilder setMaxResultsPerColumnFamily(int limit);

  GetBuilder setRowOffsetPerColumnFamily(int offset);

  GetBuilder setFilter(Filter filter);

  GetBuilder setCheckExistenceOnly(boolean checkExistenceOnly);

  GetBuilder setClosestRowBefore(boolean closestRowBefore);

  GetBuilder setAttribute(String name, byte[] value);

  GetBuilder setId(String id);

  GetBuilder setCacheBlocks(boolean cacheBlocks);

  Get build();
}
