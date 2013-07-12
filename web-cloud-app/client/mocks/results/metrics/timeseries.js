/*
 * Metrics Result Mock
 */

define([], function () {

  var sample = [
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    }
  ];

  /*
  data.eventsInRateSmall = [
    {
        'timestamp': 1372402246933,
        'value': 30000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 40000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 30000
    },
    {
        'timestamp': 1372402270620,
        'value': 35000
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 30000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 55000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
  ];

  data.eventsOutRateSmall = [
    {
        'timestamp': 1372402246933,
        'value': 55662
    },
    {
        'timestamp': 1372402252281,
        'value': 99885
    },
    {
        'timestamp': 1372402259642,
        'value': 41252
    },
    {
        'timestamp': 1372402261353,
        'value': 23123
    },
    {
        'timestamp': 1372402265483,
        'value': 33665
    },
    {
        'timestamp': 1372402270620,
        'value': 63215
    },
    {
        'timestamp': 1372402272737,
        'value': 11225
    },
    {
        'timestamp': 1372402275053,
        'value': 55663
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 66995
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 55889
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 66598
    },
  ];

  data.eventsInCountSmall = [
    {
        'timestamp': 1372402246933,
        'value': 35000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 45000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 10000
    },
    {
        'timestamp': 1372402270620,
        'value': 0
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 100000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 35000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
  ];

  data.eventsOutCountSmall = [
    {
        'timestamp': 1372402246933,
        'value': 35452
    },
    {
        'timestamp': 1372402252281,
        'value': 35852
    },
    {
        'timestamp': 1372402259642,
        'value': 45498
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 10000
    },
    {
        'timestamp': 1372402270620,
        'value': 4526
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 100000
    },
    {
        'timestamp': 1372402281606,
        'value': 85421
    },
    {
        'timestamp': 1372402283924,
        'value': 35000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
  ];

  data.eventsInRateMedium = [
    {
        'timestamp': 1372402246933,
        'value': 30000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 40000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 30000
    },
    {
        'timestamp': 1372402270620,
        'value': 35000
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 30000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 55000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
    {
        'timestamp': 1372402302254,
        'value': 40000
    },
    {
        'timestamp': 1372402369985,
        'value': 30000
    },
    {
        'timestamp': 1372402659986,
        'value': 45000
    },
    {
        'timestamp': 1372402789965,
        'value': 55000
    },
    {
        'timestamp': 1372403025632,
        'value': 60000
    },
    {
        'timestamp': 1372403569985,
        'value': 30000
    }
  ];

  data.eventsOutRateMedium = [
    {
        'timestamp': 1372402246933,
        'value': 12322
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 40000
    },
    {
        'timestamp': 1372402261353,
        'value': 32123
    },
    {
        'timestamp': 1372402265483,
        'value': 30000
    },
    {
        'timestamp': 1372402270620,
        'value': 35000
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 23221
    },
    {
        'timestamp': 1372402279489,
        'value': 30000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 55000
    },
    {
        'timestamp': 1372402288464,
        'value': 99852
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
    {
        'timestamp': 1372402302254,
        'value': 40000
    },
    {
        'timestamp': 1372402369985,
        'value': 11452
    },
    {
        'timestamp': 1372402659986,
        'value': 45000
    },
    {
        'timestamp': 1372402789965,
        'value': 66452
    },
    {
        'timestamp': 1372403025632,
        'value': 60000
    },
    {
        'timestamp': 1372403569985,
        'value': 30000
    }
  ];

  data.eventsInCountMedium = [
    {
        'timestamp': 1372402246933,
        'value': 35000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 45000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 10000
    },
    {
        'timestamp': 1372402270620,
        'value': 0
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 100000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 35000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
    {
        'timestamp': 1372402302254,
        'value': 49302
    },
    {
        'timestamp': 1372402369985,
        'value': 12322
    },
    {
        'timestamp': 1372402659986,
        'value': 12422
    },
    {
        'timestamp': 1372402789965,
        'value': 66998
    },
    {
        'timestamp': 1372403025632,
        'value': 87455
    },
    {
        'timestamp': 1372403569985,
        'value': 30000
    }
  ];

  data.eventsOutCountMedium = [
    {
        'timestamp': 1372402246933,
        'value': 35214
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 45000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 10000
    },
    {
        'timestamp': 1372402270620,
        'value': 0
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 12542
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 100000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 35000
    },
    {
        'timestamp': 1372402288464,
        'value': 65852
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
    {
        'timestamp': 1372402302254,
        'value': 49302
    },
    {
        'timestamp': 1372402369985,
        'value': 12322
    },
    {
        'timestamp': 1372402659986,
        'value': 12422
    },
    {
        'timestamp': 1372402789965,
        'value': 11245
    },
    {
        'timestamp': 1372403025632,
        'value': 87455
    },
    {
        'timestamp': 1372403569985,
        'value': 30000
    }
  ];

  data.eventsInRateLarge = [
    {
        'timestamp': 1372402246933,
        'value': 30000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 40000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 30000
    },
    {
        'timestamp': 1372402270620,
        'value': 35000
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 30000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 55000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
    {
        'timestamp': 1372402302254,
        'value': 49302
    },
    {
        'timestamp': 1372402369985,
        'value': 22556
    },
    {
        'timestamp': 1372402659986,
        'value': 12422
    },
    {
        'timestamp': 1372402789965,
        'value': 66998
    },
    {
        'timestamp': 1372403025632,
        'value': 87455
    },
    {
        'timestamp': 1372403569985,
        'value': 44556
    },
    {
        'timestamp': 1372403698856,
        'value': 49302
    },
    {
        'timestamp': 1372403856632,
        'value': 12322
    },
    {
        'timestamp': 1372404562123,
        'value': 22335
    },
    {
        'timestamp': 1372404985563,
        'value': 66998
    },
    {
        'timestamp': 1372405263321,
        'value': 87455
    },
    {
        'timestamp': 1372405965542,
        'value': 30000
    }
  ];

  data.eventsOutRateLarge = [
    {
        'timestamp': 1372402246933,
        'value': 35212
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 42121
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 30212
    },
    {
        'timestamp': 1372402270620,
        'value': 35000
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 45212
    },
    {
        'timestamp': 1372402279489,
        'value': 30000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 55000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30215
    },
    {
        'timestamp': 1372402302254,
        'value': 49302
    },
    {
        'timestamp': 1372402369985,
        'value': 22556
    },
    {
        'timestamp': 1372402659986,
        'value': 12422
    },
    {
        'timestamp': 1372402789965,
        'value': 66998
    },
    {
        'timestamp': 1372403025632,
        'value': 87455
    },
    {
        'timestamp': 1372403569985,
        'value': 44556
    },
    {
        'timestamp': 1372403698856,
        'value': 50000
    },
    {
        'timestamp': 1372403856632,
        'value': 12322
    },
    {
        'timestamp': 1372404562123,
        'value': 22335
    },
    {
        'timestamp': 1372404985563,
        'value': 66998
    },
    {
        'timestamp': 1372405263321,
        'value': 87455
    },
    {
        'timestamp': 1372405965542,
        'value': 32000
    }
  ];

  data.eventsInCountLarge = [
    {
        'timestamp': 1372402246933,
        'value': 35000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 45000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 10000
    },
    {
        'timestamp': 1372402270620,
        'value': 0
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 100000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 35000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
    {
        'timestamp': 1372402302254,
        'value': 49302
    },
    {
        'timestamp': 1372402369985,
        'value': 12322
    },
    {
        'timestamp': 1372402659986,
        'value': 12422
    },
    {
        'timestamp': 1372402789965,
        'value': 66998
    },
    {
        'timestamp': 1372403025632,
        'value': 44556
    },
    {
        'timestamp': 1372403569985,
        'value': 30000
    },
    {
        'timestamp': 1372403698856,
        'value': 49302
    },
    {
        'timestamp': 1372403856632,
        'value': 99665
    },
    {
        'timestamp': 1372404562123,
        'value': 96558
    },
    {
        'timestamp': 1372404985563,
        'value': 89755
    },
    {
        'timestamp': 1372405263321,
        'value': 11222
    },
    {
        'timestamp': 1372405965542,
        'value': 23444
    }
  ];

  data.eventsOutCountLarge = [
    {
        'timestamp': 1372402246933,
        'value': 35621
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 45000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 10000
    },
    {
        'timestamp': 1372402270620,
        'value': 85412
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 12452
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 35000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
    {
        'timestamp': 1372402302254,
        'value': 49302
    },
    {
        'timestamp': 1372402369985,
        'value': 52412
    },
    {
        'timestamp': 1372402659986,
        'value': 12422
    },
    {
        'timestamp': 1372402789965,
        'value': 66998
    },
    {
        'timestamp': 1372403025632,
        'value': 44556
    },
    {
        'timestamp': 1372403569985,
        'value': 30000
    },
    {
        'timestamp': 1372403698856,
        'value': 49302
    },
    {
        'timestamp': 1372403856632,
        'value': 99665
    },
    {
        'timestamp': 1372404562123,
        'value': 96558
    },
    {
        'timestamp': 1372404985563,
        'value': 89755
    },
    {
        'timestamp': 1372405263321,
        'value': 11222
    },
    {
        'timestamp': 1372405965542,
        'value': 23444
    }
  ];

  */

  var pathSamples = {};

  return function (path, query, callback) {

    if (pathSamples[path]) {

      var item = pathSamples[path].shift();
      pathSamples[path].push(item);

    } else {

      pathSamples[path] = $.extend(true, [], sample);

    }

    callback(200, $.extend(true, [], pathSamples[path]));

});