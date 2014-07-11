/**
 * Http mock injector for simulating responses from API.
 */
var system = {};
system.services = {
  appfabric: {},
  metrics: {},
  saver: {},
  transaction: {},
  "metrics.processor": {}
};
system.services.servicesIncompleteDefinition = require('./fixtures/system/services-incomplete.json');
system.services.servicesCompleteDefinition = require('./fixtures/system/services-complete.json');
system.services.statusIncomplete = require('./fixtures/system/services/status_incomplete.json');
system.services.statusComplete = require('./fixtures/system/services/status_complete.json');

// Appfabric service endpoints.
system.services.appfabric.status = require('./fixtures/system/services/appfabric/status.json');
system.services.appfabric.instances = require('./fixtures/system/services/appfabric/instances.json');
system.services.appfabric.logsNext = require('./fixtures/system/services/appfabric/logsNext.json');
system.services.appfabric.logsPrev = require('./fixtures/system/services/appfabric/logsPrev.json');

// Metrics service endpoints.
system.services.metrics.status = require('./fixtures/system/services/metrics/status.json');
system.services.metrics.instances = require('./fixtures/system/services/metrics/instances.json');
system.services.metrics.logsNext = require('./fixtures/system/services/metrics/logsNext.json');
system.services.metrics.logsPrev = require('./fixtures/system/services/metrics/logsPrev.json');

// Transaction service endpoints.
system.services.transaction.status = require('./fixtures/system/services/transaction/status.json');
system.services.transaction.instances = require('./fixtures/system/services/transaction/instances.json');
system.services.transaction.logsNext = require('./fixtures/system/services/transaction/logsNext.json');
system.services.transaction.logsPrev = require('./fixtures/system/services/transaction/logsPrev.json');

// Metrics processor service endpoints.
system.services['metrics.processor'].instances = require('./fixtures/system/services/metrics.processor/instances.json');

// Metrics processor service endpoints.
system.services.saver.instances = require('./fixtures/system/services/saver/instances.json');

module.exports = function (nock, gatewayAddr, gatewayPort) {

  /**
   * Set up nock environment. Disable net connection.
   */
  nock.enableNetConnect('continuuity.com');

  var clientAddr = 'http://' + gatewayAddr + ':' + gatewayPort;
  var options = {allowUnmocked: true};

  /**
   * Systems call mocks.
   */
  nock(clientAddr, options)
    .get('/v2/system/services/status').times(4)
    .reply(200, system.services.statusIncomplete);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/status')
    .reply(200, system.services.statusComplete);

  nock(clientAddr, options)
    .get('/v2/system/services').times(4)
    .reply(200, system.services.servicesIncompleteDefinition);

  nock(clientAddr, options)
    .get('/v2/system/services').times(4)
    .reply(200, system.services.servicesCompleteDefinition);

  nock(clientAddr, options)
    .get('/v2/system/services').times(2)
    .reply(200, system.services.servicesIncompleteDefinition);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services')
    .reply(200, system.services.servicesCompleteDefinition);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/appfabric/status')
    .reply(200, system.services.appfabric.status);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/appfabric/instances')
    .reply(200, system.services.appfabric.instances);

  nock(clientAddr, options)
    .persist()
    .filteringPath(function(path){
      return '/';
    })
    .get('/v2/system/services/appfabric/logs/next')
    .reply(200, system.services.appfabric.logsNext);

  nock(clientAddr, options)
    .persist()
    .filteringPath(function(path){
      return '/';
    })
    .get('/v2/system/services/appfabric/logs/prev')
    .reply(200, system.services.appfabric.logsPrev);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/metrics/status')
    .reply(200, system.services.metrics.status);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/metrics/instances')
    .reply(200, system.services.metrics.instances);

  nock(clientAddr, options)
    .persist()
    .filteringPath(function(path){
      return '/';
    })
    .get('/v2/system/services/metrics/logs/next')
    .reply(200, system.services.metrics.logsNext);

  nock(clientAddr, options)
    .persist()
    .filteringPath(function(path){
      return '/';
    })
    .get('/v2/system/services/metrics/logs/prev')
    .reply(200, system.services.metrics.logsPrev);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/transaction/status')
    .reply(200, system.services.transaction.status);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/transaction/instances')
    .reply(200, system.services.transaction.instances);

  nock(clientAddr, options)
    .persist()
    .filteringPath(function(path){
      return '/';
    })
    .get('/v2/system/services/transaction/logs/next')
    .reply(200, system.services.transaction.logsNext);

  nock(clientAddr, options)
    .persist()
    .filteringPath(function(path){
      return '/';
    })
    .get('/v2/system/services/transaction/logs/prev')
    .reply(200, system.services.transaction.logsPrev);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/saver/instances')
    .reply(200, system.services.saver.instances);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/metrics.processor/instances')
    .reply(200, system.services['metrics.processor'].instances);

};