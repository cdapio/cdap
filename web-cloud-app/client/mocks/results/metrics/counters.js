/*
 * Counter Metrics Result Mock
 */

define([], function () {

  var sample = {

    // MapReduce
    "/process/completion/CountAndFilterWords/mapreduce/batchid1/mappers": 29,
    "/process/completion/CountAndFilterWords/mapreduce/batchid1/reducers": 30,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/mappers/ins": 3752,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/mappers/outs": 35654,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/reducers/ins": 21,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/reducers/outs": 25632
  };

  return function (path, query, callback) {

    callback(200, sample[path]);

  };

});