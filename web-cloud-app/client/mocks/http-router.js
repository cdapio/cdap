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
    return path in this.pathRouter ? this.pathRouter[path] : null;
  };

  httpRouter.pathRouter = {

    '/batch/SampleApplicationId:batchid1': Samples.batchSample,
    '/batch/SampleApplicationId:batchid1?data=metrics': Counters.batchMetrics,
    '/batch/SampleApplicationId:batchid1?data=alerts': Counters.batchAlerts,
    '/metrics/events_in?format=rate&duration=7': TimeSeries.eventsInRateSmall,
    '/metrics/events_in?format=count&duration=7': TimeSeries.eventsInCountSmall,
    '/metrics/events_in?format=rate&duration=14':  TimeSeries.eventsInRateMedium,
    '/metrics/events_in?format=count&duration=14': TimeSeries.eventsInCountMedium,
    '/metrics/events_in?format=rate&duration=30': TimeSeries.eventsInRateLarge,
    '/metrics/events_in?format=count&duration=30': TimeSeries.eventsInCountLarge
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