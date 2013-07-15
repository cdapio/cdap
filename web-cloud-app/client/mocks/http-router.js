/*
 * Request router.
 * This matches requests to responses and provides a sample response. Method names from
 * request should map to function names on this router.
 */

define(['mocks/results/metrics/timeseries', 'mocks/results/metrics/counters',
        'mocks/results/metrics/samples'],
 function (TimeSeries, Counters, Samples) {

  var httpRouter = {};

  httpRouter.getResult = function(path) {

    if (path in this.pathRouter) {

      var sample = this.pathRouter[path];
      if (typeof sample === 'number') {
        return sample;
      }
      if (typeof sample === 'object') {
        if (sample.length) {
          return $.extend(true, [], sample);
        }
        return $.extend(true, {}, sample);
      }

    }
    return null;

  };

  httpRouter.pathRouter = {
    '/version': '1.5.0',
    '/disk': { free: 1024 },

    // REST
    '/rest/apps': Samples.applicationsSample,
    '/rest/streams': Samples.streamsSample,
    '/rest/flows': Samples.flowsSample,
    '/rest/mapreduce': Samples.batchesSample,
    '/rest/datasets': Samples.datasetsSample,
    '/rest/procedures': Samples.proceduresSample,

    '/rest/streams/text': Samples.streamSample,

    '/rest/streams/wordStream': Samples.streamSample,
    '/rest/streams/wordStream/flows': Samples.flowsSample,

    '/rest/datasets/wordAssocs': Samples.datasetSample,
    '/rest/datasets/uniqueCount': Samples.datasetSample,
    '/rest/datasets/wordCounts': Samples.datasetSample,
    '/rest/datasets/wordStats': Samples.datasetSample,
    '/rest/datasets/filterTable': Samples.datasetSample,

    '/rest/datasets/wordAssocs/flows': Samples.flowsSample,
    '/rest/datasets/uniqueCount/flows': Samples.flowsSample,
    '/rest/datasets/wordCounts/flows': Samples.flowsSample,
    '/rest/datasets/wordStats/flows': Samples.flowsSample,
    '/rest/datasets/filterTable/flows': Samples.flowsSample,

    '/rest/apps/WordCount': Samples.applicationSample,
    '/rest/apps/WordCount/streams': Samples.streamsSample,
    '/rest/apps/WordCount/flows': Samples.flowsSample,
    '/rest/apps/WordCount/datasets': Samples.datasetsSample,
    '/rest/apps/WordCount/procedures': Samples.proceduresSample,
    '/rest/apps/WordCount/flows/CountRandom': Samples.flowDefinitionSample,
    '/rest/apps/WordCount/flows/CountAndFilterWords': Samples.flowDefinitionSample,
    '/rest/apps/WordCount/flows/WordCounter': Samples.flowDefinitionSample,
    '/rest/apps/WordCount/procedures/RetrieveCounts': Samples.procedureSample,

    '/rest/apps/CountRandom/flows/CountAndFilterWords': Samples.flowDefinitionSample,

    '/rest/apps/CountRandom': Samples.applicationSample,
    '/rest/apps/CountRandom/flows/CountRandom': Samples.flowDefinitionSample,

    '/rest/apps/CountAndFilterWords': Samples.applicationSample,
    '/rest/apps/CountAndFilterWords/streams': Samples.streamsSample,
    '/rest/apps/CountAndFilterWords/flows': Samples.flowsSample,
    '/rest/apps/CountAndFilterWords/mapreduce': Samples.batchesSample,
    '/rest/apps/CountAndFilterWords/datasets': Samples.datasetsSample,
    '/rest/apps/CountAndFilterWords/procedures': Samples.proceduresSample,
    '/rest/apps/CountAndFilterWords/flows/CountAndFilterWords': Samples.flowDefinitionSample,
    '/rest/apps/CountAndFilterWords/flows/CountRandom': Samples.flowDefinitionSample,
    '/rest/apps/CountAndFilterWords/mapreduce/batchid1': Samples.batchSample,
    '/rest/apps/CountAndFilterWords/mapreduce/batchsampleid1': Samples.batchSample,

    // RPC
    '/rpc/runnable/status': { result: { status: 'STOPPED' }},
    '/rpc/runnable/start': { result: true },
    '/rpc/runnable/stop': { result: true },
    '/rpc/runnable/setInstances': { result: true },
    '/rpc/runnable/getFlowHistory': { params: [] }

  };

  /*
  metrics = {
    'process/busyness/app1':
    'process/busyness/flows/flow1':
    'process/busyness/flowlets/flowlet1'
    'process/busyness/jobs/job1/mappers/mapper1'
    'process/busyness/jobs/job1/reducers/reducer1'
  };
  */

  return httpRouter;

});