/*
 * Socket request router
 */

define(['mocks/results/metrics/timeseries'], function (TimeSeries) {

  return {
    "getTimeSeries" :  TimeSeries.timeSeriesSample,
    "getCounters": TimeSeries.counterSample
  };

});