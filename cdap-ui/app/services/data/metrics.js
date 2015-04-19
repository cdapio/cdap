angular.module(PKG.name + '.services')
  .factory('MyMetrics', function MyMetrics() {

    // 'ns.default.app.foo' -> {'ns': 'default', 'app': 'foo'}
    function contextToTags(context) {
      var parts, tags, i;
      parts = context.split('.');
      if (parts.length % 2 != 0) {
        // TODO: this should be checked when user is creating the widget.
        throw "Metrics context has uneven number of parts: " + this.metric.context
      }
      tags = {};
      for (i = 0; i < parts.length; i+=2) {
        tags[parts[i]] = parts[i + 1]
      }
      return tags;
    }

    function cleanMetricName(metricName) {
      // http://stackoverflow.com/questions/20306204/using-queryselector-with-ids-that-are-numbers
      // http://www.w3.org/TR/CSS21/syndata.html#value-def-identifier
      // Replace all invalid characters with '_'. This is ok for now, since we do not display the chart labels
      // to the user. Source: http://stackoverflow.com/questions/13979323/how-to-test-if-selector-is-valid-in-jquery

      // Updated to replace '.' only if preceded by a number.
      return metricName.replace(/([;&,\+\*\~':"\!\^#$%@\[\]\(\)=><\|])/g, '_').replace(/\.\d/, '_');
    }

    function constructQuery(queryId, tags, metrics) {
      var timeRange, retObj;
      timeRange = {'start': 'now-60s', 'end': 'now'};
      retObj = {};
      retObj[queryId] = {tags: tags, metrics: metrics, groupBy: [], timeRange: timeRange};
      return retObj;
    }

    function mapMetrics(metricValuesMap) {
      var vs = [];
      for (var i = 0; i < metricValuesMap.length; i++) {
        metricMap = metricValuesMap[i];
        vs.push(Object.keys(metricMap).map(function(key) {
          return {
            time: key,
            y: metricMap[key]
          };
        }));
      }
      return vs;
    }

    function aggregate(arr) {
      var count = 0;
      for (var i = 0; i < arr.length; i++) {
        count += arr[i].y;
      }
      return count;
    }

    return {
      contextToTags: contextToTags,
      cleanMetricName: cleanMetricName,
      constructQuery: constructQuery,
      mapMetrics: mapMetrics,
      aggregate: aggregate
    };
  });
