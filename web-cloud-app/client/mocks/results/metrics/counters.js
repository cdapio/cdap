/*
 * Counter Metrics Result Mock
 */

define([], function () {

  var sample = {

    // MapReduce
    '/store/bytes/datasets/dataset1': 37.34,
    '/store/records/datasets/dataset1': 117,
    '/process/completion/jobs/mappers/job1': 75,
    '/process/events/jobs/mappers/job1': 100,
    '/process/bytes/jobs/mappers/job1': 30.00,
    '/process/completion/jobs/reducers/job1': 0,
    '/process/events/jobs/reducers/job1': 0,
    '/process/bytes/jobs/reducers/job1': 0,
    '/store/bytes/datasets/dataset2': 0,
    '/store/records/datasets/dataset2': 0,

    // Overview
  };

  return function (path, query, callback) {

    callback(200, sample[path]);

  };

});