/**
 *  Sample responses for dashboard and application. Responses determining the structure of the
 * application must be defined here.
 */

define([], function() {

  return {
    "appMetrics": [
      {
        'name': 'Events Processed',
        'path': '/process/events/{id}'
      },
      {
        'name': 'Busyness',
        'path': '/process/busyness/{id}'
      }
    ],
    "flowsByStreamSample": [
      {
        "id": "WordCounter",
        "application": "WordCount",
        "name": "WordCounter",
        "streams": [
          "wordStream"
        ],
        "datasets": [
          "wordCounts",
          "wordStats",
          "wordAssocs",
          "uniqueCount"
        ],
        "exists": true
      }
    ],
    "statusSample": {
      "applicationId": "CountAndFilterWords",
      "flowId": "CountAndFilterWords",
      "version": 1,
      "runId": null,
      "status": "STOPPED"
    },
    "applicationSample": {
      "id": "CountAndFilterWords",
      "name": "CountAndFilterWords",
      "description": "Example word filter and count application",
      "exists": true
    },
    "applicationsSample": [
      {
        "id": "CountAndFilterWords",
        "name": "CountAndFilterWords",
        "description": "Example word filter and count application",
        "exists": true,
        "type": "App"
      },
      {
        "id": "WordCount",
        "name": "WordCount",
        "description": "Example Word Count Application",
        "exists": true,
        "type": "App"
      }
    ],
    "streamsSample": [
      {
        "id": "text",
        "name": "text",
        "description": null,
        "capacityInBytes": null,
        "expiryInSeconds": null,
        "exists": true
      },
      {
        "id": "wordStream",
        "name": "wordStream",
        "description": null,
        "capacityInBytes": null,
        "expiryInSeconds": null,
        "exists": true
     }
    ],
    "streamSample": {
      "id": "wordStream",
      "name": "wordStream",
      "description": null,
      "capacityInBytes": null,
      "expiryInSeconds": null,
      "exists": true
    },
    "flowSample": {
          "meta": {
            "name": "CountAndFilterWords",
            "app": "CountAndFilterWords"
          },
          "id": "CountAndFilterWords",
          "application": "CountAndFilterWords",
          "name": "CountAndFilterWords",
          "streams": [
              "text"
          ],
          "datasets": [
              "filterTable"
          ],
          "exists": true
      },
    "flowsSample": [
      {
          "id": "CountAndFilterWords",
          "application": "CountAndFilterWords",
          "name": "CountAndFilterWords",
          "streams": [
              "text"
          ],
          "datasets": [
              "filterTable"
          ],
          "exists": true
      },
      {
          "id": "CountRandom",
          "application": "CountRandom",
          "name": "CountRandom",
          "streams": [],
          "datasets": [
              "randomTable"
          ],
          "exists": true
      },
      {
          "id": "WordCounter",
          "application": "WordCount",
          "name": "WordCounter",
          "streams": [
              "wordStream"
          ],
          "datasets": [
              "wordCounts",
              "wordStats",
              "wordAssocs",
              "uniqueCount"
          ],
          "exists": true
      }
    ],
    "batchesSample": [
      {
          "id": "batchsampleid1",
          "application": "CountAndFilterWords",
          "name": "batchsamplename1",
          "streams": [
              "text"
          ],
          "datasets": [
              "filterTable"
          ],
          "exists": true
      }
    ],
    "procedureSample": {
        "id": "RetrieveCounts",
        "application": "WordCount",
        "name": "RetrieveCounts",
        "description": null,
        "serviceName": "RetrieveCounts",
        "datasets": [],
        "exists": true
    },
    "proceduresSample": [
      {
          "id": "RetrieveCounts",
          "application": "WordCount",
          "name": "RetrieveCounts",
          "description": null,
          "serviceName": "RetrieveCounts",
          "datasets": [],
          "exists": true
      }
    ],
    "datasetsSample": [
      {
          "id": "filterTable",
          "name": "filterTable",
          "description": "",
          "type": "com.continuuity.api.data.dataset.KeyValueTable",
          "exists": true
      },
      {
          "id": "uniqueCount",
          "name": "uniqueCount",
          "description": "",
          "type": "com.continuuity.examples.wordcount.UniqueCountTable",
          "exists": true
      },
      {
          "id": "wordAssocs",
          "name": "wordAssocs",
          "description": "",
          "type": "com.continuuity.examples.wordcount.AssociationTable",
          "exists": true
      },
      {
          "id": "wordCounts",
          "name": "wordCounts",
          "description": "",
          "type": "com.continuuity.api.data.dataset.KeyValueTable",
          "exists": true
      },
      {
          "id": "wordStats",
          "name": "wordStats",
          "description": "",
          "type": "com.continuuity.api.data.dataset.table.Table",
          "exists": true
      }
    ],
    "datasetSample": {
          "id": "filterTable",
          "name": "filterTable",
          "description": "",
          "type": "com.continuuity.api.data.dataset.KeyValueTable",
          "exists": true
      },
    "streamsByApplicationSample": [
      {
          "id": "wordStream",
          "name": "wordStream",
          "description": null,
          "capacityInBytes": null,
          "expiryInSeconds": null,
          "exists": true
      }
    ],
    "flowsByApplicationSample": [
      {
          "id": "WordCounter",
          "application": "WordCount",
          "name": "WordCounter",
          "streams": [
              "wordStream"
          ],
          "datasets": [
              "wordCounts",
              "wordStats",
              "wordAssocs",
              "uniqueCount"
          ],
          "exists": true
      }
    ],
    "datasetsByApplicationSample": [
      {
          "id": "wordCounts",
          "name": "wordCounts",
          "description": "",
          "type": "com.continuuity.api.data.dataset.KeyValueTable",
          "exists": true
      },
      {
          "id": "wordStats",
          "name": "wordStats",
          "description": "",
          "type": "com.continuuity.api.data.dataset.table.Table",
          "exists": true
      },
      {
          "id": "uniqueCount",
          "name": "uniqueCount",
          "description": "",
          "type": "com.continuuity.examples.wordcount.UniqueCountTable",
          "exists": true
      },
      {
          "id": "wordAssocs",
          "name": "wordAssocs",
          "description": "",
          "type": "com.continuuity.examples.wordcount.AssociationTable",
          "exists": true
      }
    ],
    "queriesByApplicationSample": [
      {
          "id": "RetrieveCounts",
          "application": "WordCount",
          "name": "RetrieveCounts",
          "description": null,
          "serviceName": "RetrieveCounts",
          "datasets": [],
          "exists": true
      }
    ],
    "flowDefinitionSample": {
          "meta": {
              "name": "CountAndFilterWords",
              "app": "CountAndFilterWords"
          },
          "datasets": [
              "filterTable"
          ],
          "flowlets": [
              {
                  "name": "number-filter",
                  "classname": "com.continuuity.examples.countandfilterwords.DynamicFilterFlowlet",
                  "instances": 1,
                  "id": 0,
                  "type": "Compute",
                  "groupId": 0
              },
              {
                  "name": "count-all",
                  "classname": "com.continuuity.examples.countandfilterwords.Counter",
                  "instances": 1,
                  "id": 0,
                  "type": "Sink",
                  "groupId": 0
              },
              {
                  "name": "count-number",
                  "classname": "com.continuuity.examples.countandfilterwords.Counter",
                  "instances": 1,
                  "id": 0,
                  "type": "Sink",
                  "groupId": 0
              },
              {
                  "name": "count-lower",
                  "classname": "com.continuuity.examples.countandfilterwords.Counter",
                  "instances": 1,
                  "id": 0,
                  "type": "Sink",
                  "groupId": 0
              },
              {
                  "name": "lower-filter",
                  "classname": "com.continuuity.examples.countandfilterwords.DynamicFilterFlowlet",
                  "instances": 1,
                  "id": 0,
                  "type": "Compute",
                  "groupId": 0
              },
              {
                  "name": "upper-filter",
                  "classname": "com.continuuity.examples.countandfilterwords.DynamicFilterFlowlet",
                  "instances": 1,
                  "id": 0,
                  "type": "Compute",
                  "groupId": 0
              },
              {
                  "name": "source",
                  "classname": "com.continuuity.examples.countandfilterwords.StreamSource",
                  "instances": 1,
                  "id": 0,
                  "type": "Compute",
                  "groupId": 0
              },
              {
                  "name": "count-upper",
                  "classname": "com.continuuity.examples.countandfilterwords.Counter",
                  "instances": 1,
                  "id": 0,
                  "type": "Sink",
                  "groupId": 0
              },
              {
                  "name": "splitter",
                  "classname": "com.continuuity.examples.countandfilterwords.Tokenizer",
                  "instances": 1,
                  "id": 0,
                  "type": "Compute",
                  "groupId": 0
              }
          ],
          "connections": [
              {
                  "from": {
                      "flowlet": "splitter",
                      "stream": "tokens_out"
                  },
                  "to": {
                      "flowlet": "number-filter",
                      "stream": "tokens_in"
                  }
              },
              {
                  "from": {
                      "flowlet": "splitter",
                      "stream": "counts_out"
                  },
                  "to": {
                      "flowlet": "count-all",
                      "stream": "counts_in"
                  }
              },
              {
                  "from": {
                      "flowlet": "splitter",
                      "stream": "tokens_out"
                  },
                  "to": {
                      "flowlet": "lower-filter",
                      "stream": "tokens_in"
                  }
              },
              {
                  "from": {
                      "flowlet": "splitter",
                      "stream": "tokens_out"
                  },
                  "to": {
                      "flowlet": "upper-filter",
                      "stream": "tokens_in"
                  }
              },
              {
                  "from": {
                      "flowlet": "upper-filter",
                      "stream": "counts_out"
                  },
                  "to": {
                      "flowlet": "count-upper",
                      "stream": "counts_in"
                  }
              },
              {
                  "from": {
                      "flowlet": "source",
                      "stream": "queue_out"
                  },
                  "to": {
                      "flowlet": "splitter",
                      "stream": "queue_in"
                  }
              },
              {
                  "from": {
                      "flowlet": "number-filter",
                      "stream": "counts_out"
                  },
                  "to": {
                      "flowlet": "count-number",
                      "stream": "counts_in"
                  }
              },
              {
                  "from": {
                      "stream": "text"
                  },
                  "to": {
                      "flowlet": "source",
                      "stream": "text_in"
                  }
              },
              {
                  "from": {
                      "flowlet": "lower-filter",
                      "stream": "counts_out"
                  },
                  "to": {
                      "flowlet": "count-lower",
                      "stream": "counts_in"
                  }
              }
          ],
          "flowStreams": [
              {
                  "name": "text"
              }
          ],
          "flowletStreams": {
              "number-filter": {
                  "counts_out": {
                      "first": "queue://CountAndFilterWords/number-filter/counts_out",
                      "second": "OUT"
                  },
                  "tokens_in": {
                      "first": "queue://CountAndFilterWords/splitter/tokens_out",
                      "second": "IN"
                  }
              },
              "count-all": {
                  "counts_in": {
                      "first": "queue://CountAndFilterWords/splitter/counts_out",
                      "second": "IN"
                  }
              },
              "count-lower": {
                  "counts_in": {
                      "first": "queue://CountAndFilterWords/lower-filter/counts_out",
                      "second": "IN"
                  }
              },
              "count-number": {
                  "counts_in": {
                      "first": "queue://CountAndFilterWords/number-filter/counts_out",
                      "second": "IN"
                  }
              },
              "lower-filter": {
                  "counts_out": {
                      "first": "queue://CountAndFilterWords/lower-filter/counts_out",
                      "second": "OUT"
                  },
                  "tokens_in": {
                      "first": "queue://CountAndFilterWords/splitter/tokens_out",
                      "second": "IN"
                  }
              },
              "upper-filter": {
                  "counts_out": {
                      "first": "queue://CountAndFilterWords/upper-filter/counts_out",
                      "second": "OUT"
                  },
                  "tokens_in": {
                      "first": "queue://CountAndFilterWords/splitter/tokens_out",
                      "second": "IN"
                  }
              },
              "source": {
                  "text_in": {
                      "first": "stream://developer/text",
                      "second": "IN"
                  },
                  "queue_out": {
                      "first": "queue://CountAndFilterWords/source/queue_out",
                      "second": "OUT"
                  }
              },
              "count-upper": {
                  "counts_in": {
                      "first": "queue://CountAndFilterWords/upper-filter/counts_out",
                      "second": "IN"
                  }
              },
              "splitter": {
                  "queue_in": {
                      "first": "queue://CountAndFilterWords/source/queue_out",
                      "second": "IN"
                  },
                  "counts_out": {
                      "first": "queue://CountAndFilterWords/splitter/counts_out",
                      "second": "OUT"
                  },
                  "tokens_out": {
                      "first": "queue://CountAndFilterWords/splitter/tokens_out",
                      "second": "OUT"
                  }
              }
          }
      },
      "flowsByDatasetSample": [
          {
              "id": "CountAndFilterWords",
              "application": "CountAndFilterWords",
              "name": "CountAndFilterWords",
              "streams": [
                  "text"
              ],
              "datasets": [
                  "filterTable"
              ],
              "exists": true
          }
      ],
      "querySample": {
        "id": "RetrieveCounts",
        "application": "WordCount",
        "name": "RetrieveCounts",
        "description": null,
        "serviceName": "RetrieveCounts",
        "datasets": [],
        "exists": true
      },
      "batchSample": {
        "meta": {
          "name": "BatchJobName1",
          "app": "CountAndFilterWords",
          "startTime": "1371248384775"
        },
        "id": "batchid1",
        "application": "CountAndFilterWords",
        "name": "BatchJobName1",
        "streams": [
            "text"
        ],
        "datasets": [
            "filterTable"
        ],
        "exists": true
      }
  };
});