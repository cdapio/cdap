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

package co.cask.cdap.explore.executor;

import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryInfo;
import co.cask.cdap.proto.QueryResult;
import co.cask.http.AbstractHttpHandler;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An abstract class that provides common functionality for namespaced and non-namespaced ExploreQuery handlers.
 */
public class AbstractQueryExecutorHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractQueryExecutorHttpHandler.class);
  protected static final Gson GSON = new Gson();
  protected static final int DOWNLOAD_FETCH_CHUNK_SIZE = 1000;

  protected static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();


  protected List<QueryInfo> filterQueries(List<QueryInfo> queries, final long offset,
                                          final boolean isForward, final int limit) {
    // Reverse the list if the pagination is in the reverse from the offset until the max limit
    if (!isForward) {
      queries = Lists.reverse(queries);
    }

    return FluentIterable.from(queries)
      .filter(new Predicate<QueryInfo>() {
        @Override
        public boolean apply(@Nullable QueryInfo queryInfo) {
          if (isForward) {
            return queryInfo.getTimestamp() < offset;
          } else {
            return queryInfo.getTimestamp() > offset;
          }
        }
      })
      .limit(limit)
      .toSortedImmutableList(new Comparator<QueryInfo>() {
        @Override
        public int compare(QueryInfo first, QueryInfo second) {
          //sort descending.
          return Longs.compare(second.getTimestamp(), first.getTimestamp());
        }
      });
  }

  // get arguments contained in the request body
  protected Map<String, String> decodeArguments(HttpRequest request) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return ImmutableMap.of();
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      Map<String, String> args = GSON.fromJson(reader, STRING_MAP_TYPE);
      return args == null ? ImmutableMap.<String, String>of() : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }

  protected String getCSVHeaders(List<ColumnDesc> schema)
    throws HandleNotFoundException, SQLException, ExploreException {
    StringBuffer sb = new StringBuffer();
    boolean first = true;
    for (ColumnDesc columnDesc : schema) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append(columnDesc.getName());
    }
    return sb.toString();
  }

  protected String appendCSVRow(StringBuffer sb, QueryResult result)
    throws HandleNotFoundException, SQLException, ExploreException {
    boolean first = true;
    for (Object o : result.getColumns()) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      // Using GSON toJson will serialize objects - in particular, strings will be quoted
      sb.append(GSON.toJson(o));
    }
    return sb.toString();
  }

}
