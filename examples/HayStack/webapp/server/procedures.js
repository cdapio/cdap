
var http = require('http');

var config = null;

function request (path, body, callback) {

  if (config === null) {

    throw('Procedure configuration is empty.');

  }

  var content = JSON.stringify(body);

  var options = {
      host: config['reactor.hostname'],
      port: config['reactor.gateway.port'],
      path: '/v2/apps' + path,
      method: 'GET',
//      headers: {
//        'Content-Type': 'application/json',
//        'Content-Length': content.length
//      }
    };

  var request = http.request(options, function(response) {
    var data = '';
    response.on('data', function (chunk) {
      data += chunk;
    });
    
    console.log(response);

    response.on('end', function () {

      try {
        if (data === '') {
          callback({
            error: 'Please make sure the Reactor application is running.'
          });
        } else {
          callback(JSON.parse(data));
        }
      } catch (e) {
        callback({
          error: e.message + ': "' + data + '"'
        });
      }

    });
  });

  request.on('error', function (e) {

    callback({
      error: 'Could not connect to Reactor. Please check your configuration.'
    });

  });

  request.write(content);
  request.end();

}

module.exports = {

  configure: function (fromFile) {

    config = fromFile;

  },

  Logs: {

    getLogs: function (params, response) {

      request('/personalization/procedures/ranker/methods/rank', params, function (result) {

        response.send([{ message: 'I am a log message' }]);

      });

    }

  },
  
  Search: {
  
    search: function (query, response) {

      var params = [];
      for (var k in query) {
         console.log(query[k]);
         params.push(k + "=" + query[k]);
      }
      
      var url = "/Haystack/procedures/Search/methods/timeBoundSearch?" + params.join('&');
      request(url, null, function (result) {

        response.send(result);

      });
      
      }

    },

  Trend: {
  
    trend: function (query, response) {

      var params = [];
      for (var k in query) {
         console.log(query[k]);
         params.push(k + "=" + query[k]);
      }
      
      var url = "/Haystack/procedures/Analytics/methods/logLevelStats?" + params.join('&');
      request(url, params, function (result) {

        response.send(result);

      });

    }
  },

  Alerts: {
  
    alerts: function (query, response) {

      var params = [];
      for (var k in query) {
         console.log(query[k]);
         params.push(k + "=" + query[k]);
      }
      
      var url = "/Haystack/procedures/Alerts/methods/triggered?" + params.join('&');
      request(url, params, function (result) {

        response.send(result);

      });

    }
  }

};
