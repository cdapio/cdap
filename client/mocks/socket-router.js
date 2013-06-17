/*
 * Request router.
 * This matches requests to responses and provides a sample response. Method names from
 * request should map to function names on this router.
 */

define(['mocks/results/metrics/timeseries', 'mocks/results/metrics/counters',
        'mocks/results/metrics/samples'],
 function (TimeSeries, Counters, Samples) {

  return {
    getTimeSeries : function(request) {
      var response = {
        id: request.id,
        method: request.method,
        params: {
          points: {},
          latest: null
        }
      };
      if (request.params[2]) {
        for (var i = 0, len = request.params[2].length; i < len; i++) {
          TimeSeries('', { start: 0, end: 0, count: 60 }, function (status, result) {
            response.params.points[request.params[2][i]] = result;
          });
        }
      }
      return response;
    },
    getCounters: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Counters.counterSample
      };
    },
    getFlowsByStream: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.flowsByStreamSample
      };
    },
    getFlowsByApplication: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.flowsByApplicationSample
      };
    },
    getDatasetsByApplication: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.datasetsByApplicationSample
      };
    },
    getQueriesByApplication: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.queriesByApplicationSample
      };
    },
    getStreamsByApplication: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.streamsByApplicationSample
      };
    },
    status: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.statusSample
      };
    },
    getFlowDefinition: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.flowDefinitionSample
      };
    },
    getApplication: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.applicationSample
      };
    },
    getFlows: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.flowsSample
      };
    },
    getStreams: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.streamsSample
      };
    },
    getQueries: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.queriesSample
      };
    },
    getDatasets: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.datasetsSample
      };
    },
    getFlowsByDataset: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.flowsByDatasetSample
      };
    },
    getQuery: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.querySample
      };
    },
    getBatch: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Samples.batchSample
      };
    },
    getBatchMetrics: function(request) {
      return {
        id: request.id,
        method: request.method,
        params: Counters.batchMetricCounters
      };
    }
  };

});