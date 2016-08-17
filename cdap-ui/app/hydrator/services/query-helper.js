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

angular.module(PKG.name + '.feature.hydrator')
  .factory('MyMetricsQueryHelper', function() {
    // 'ns.default.app.foo' -> {'ns': 'default', 'app': 'foo'}
    // returns null if context argument is malformed (odd number of parts)
    function contextToTags(context) {
      var parts, tags, i, tagValue;
      if (context.length) {
        parts = context.split('.');
      } else {
        // For an empty context, we want no tags. Splitting it by '.' yields [""]
        parts = [];
      }
      if (parts.length % 2 !== 0) {
        // Metrics context must have even number of parts
        return null;
      }
      tags = {};
      for (i = 0; i < parts.length; i+=2) {
        // In context, '~' is used to represent '.'
        tagValue = parts[i + 1].replace(/~/g, '.');
        tags[parts[i]] = tagValue;
      }
      return tags;
    }

    // TODO: Need to figure out a way to pass url for a chart
    // that is part of the widget, which is not a metric.
    // Right now a chart and a metric is tied together and
    // it needs to be changed.
    function constructQuery(queryId, tags, metric, isTimeRange, groupBy) {
      var timeRange, retObj;
      timeRange = {
        'start': metric.startTime || 'now-60s',
        'end': metric.endTime || 'now'
      };
      if (metric.resolution) {
        timeRange.resolution = metric.resolution;
      }
      retObj = {};
      retObj[queryId] = {
        tags: tags,
        metrics: metric.names
      };
      groupBy = groupBy || [];
      if (groupBy.length) {
        retObj[queryId].groupBy = groupBy;
      }

      isTimeRange = (isTimeRange !== false);
      if (isTimeRange) {
        retObj[queryId].timeRange = timeRange;
      }
      return retObj;
    }

    function roundUpToNearest(val, nearest) {
      return Math.ceil(val / nearest) * nearest;
    }

    function roundDownToNearest(val, nearest) {
      return Math.floor(val / nearest) * nearest;
    }

    function aggregate(inputMetrics, by) {
      // Given an object in the format: { ts1: value, ts2: value, ts3: value, ts4: value },
      // This will return an object in the same format, where each sequence of {by} timestamps will be summed up.
      // Not currently considering resolution of the metric values (It groups simply starting from the first timestamp),
      // as opposed to grouping into 5-minute interval.
      var aggregated = {};
      var timeValues = Object.keys(inputMetrics);
      var roundedDown = roundDownToNearest(timeValues.length, by);
      for (var i = 0; i < roundedDown; i += by) {
        var sum = 0;
        for (var j = 0; j < by; j++) {
          sum += inputMetrics[timeValues[i + j]];
        }
        aggregated[timeValues[i]] = sum;
      }
      // Add up remainder elements (in case number of elements in obj is not evenly divisible by {by}
      if (roundedDown < timeValues.length) {
        var finalKey = timeValues[roundedDown];
        aggregated[finalKey] = 0;
        for (i = roundedDown; i < timeValues.length; i++) {
          aggregated[finalKey] += inputMetrics[timeValues[i]];
        }
      }
      return aggregated;
    }

    // {name: k1, value: v1} -> 'k1.v2'
    function tagToContext(tag) {
      var key = tag.name.replace(/\./g, '~');
      var value = tag.value.replace(/\./g, '~');
      return key + '.' + value;
    }

    // { namespace: default, app: foo, flow: bar } -> 'tag=namespace:default&tag=app:foo&tag=flow:bar'
    function tagsToParams(tags) {
      var keys = Object.keys(tags);
      var queryParams = [];
      keys.forEach(function(key) {
        var value = tags[key];
        queryParams.push('tag=' + key + ':' + value);
      });
      return queryParams.join('&');
    }

    return {
      contextToTags: contextToTags,
      constructQuery: constructQuery,
      roundUpToNearest: roundUpToNearest,
      roundDownToNearest: roundDownToNearest,
      aggregate: aggregate,
      tagToContext: tagToContext,
      tagsToParams: tagsToParams
    };
  });
