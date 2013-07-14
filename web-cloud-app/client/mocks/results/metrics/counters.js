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
    '/process/events/flowlets/number-filter': 10,
    '/process/events/flowlets/count-all': 10,
    '/process/events/flowlets/count-number': 10,
    '/process/events/flowlets/count-lower': 10,
    '/process/events/flowlets/lower-filter': 10,
    '/process/events/flowlets/upper-filter': 10,
    '/process/events/flowlets/source': 10,
    '/process/events/flowlets/count-upper': 10,
    '/process/events/flowlets/splitter': 10,

    // Store
    '/store/bytes/datasets/filterTable': 100000,
    '/store/bytes/datasets/uniqueCount': 2000,
    '/store/bytes/datasets/wordAssocs': 2928312,
    '/store/bytes/datasets/wordCounts': 19232,
    '/store/bytes/datasets/wordStats': 100000

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