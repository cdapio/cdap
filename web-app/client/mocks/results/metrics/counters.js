/*
 * Counter Metrics Result Mock
 */

define([], function () {

  var sample = {

    // MapReduce
    '/store/bytes/datasets/dataset1': 37.34,
    '/store/records/datasets/dataset1': 117,
    '/process/completion/jobs/mappers/batchid1': 75,
    '/process/events/jobs/mappers/batchid1': 100,
    '/process/bytes/jobs/mappers/batchid1': 30.00,
    '/process/completion/jobs/reducers/batchid1': 0,
    '/process/events/jobs/reducers/batchid1': 0,
    '/process/bytes/jobs/reducers/batchid1': 0,
    '/store/bytes/datasets/filterTable': 0,
    '/store/records/datasets/filterTable': 0,

    // Overview

    '/collect/bytes/streams/wordStream': 10,
    '/collect/bytes/streams/text': 10,
    '/collect/events/streams/text': 10,
    '/collect/events/streams/wordStream': 10,

    // App Storage
    '/store/bytes/CountAndFilterWords': 40,

    // Flowlets
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/number-filter': 100,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/count-all': 100,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/count-number': 100,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/count-lower': 50,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/lower-filter': 45030304,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/upper-filter': 10,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/source': 24,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/count-upper': 100,
    '/process/events/CountAndFilterWords/flows/CountAndFilterWords/splitter': 100,

    // Store
    '/store/bytes/datasets/filterTable': 100000,
    '/store/bytes/datasets/uniqueCount': 2000,
    '/store/bytes/datasets/wordAssocs': 2928312,
    '/store/bytes/datasets/wordCounts': 19232,
    '/store/bytes/datasets/wordStats': 100000,

    "/process/completion/CountAndFilterWords/mapreduce/batchid1/mappers": 29,
    "/process/completion/CountAndFilterWords/mapreduce/batchid1/reducers": 30,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/mappers/ins": 3752,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/mappers/outs": 35654,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/reducers/ins": 21,
    "/process/entries/CountAndFilterWords/mapreduce/batchid1/reducers/outs": 25632
  };

  return function (path, query, callback) {

    callback(200, {
        path: path,
        result: {
            data: sample[path]
        },
        error: null
    });

  };

});
