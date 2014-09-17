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

// User services endpoints.
var apps = {};
apps.CountCounts = {
  services: {
    loadgen: {}
  }
};
apps.CountCounts.services.servicesCompleteDefinition  = require('./fixtures/user/services-complete.json');
apps.CountCounts.services.loadgen                     = require('./fixtures/user/services/service-spec.json');
apps.CountCounts.services.loadgen.instances           = require('./fixtures/user/services/instances.json');
apps.CountCounts.services.loadgen.status              = require('./fixtures/user/services/status.json');
apps.CountCounts.services.loadgen.runtimeargs        = require('./fixtures/user/services/runtime-args.json');
apps.CountCounts.services.loadgen.history             = require('./fixtures/user/services/history.json');
apps.CountCounts.services.loadgen.liveinfo           = require('./fixtures/user/services/live-info.json');

var servicesinfo_countcounts = require('./fixtures/user/user-services-batch-complete-countcounts.json');
var servicesinfo_purchasehistory = require('./fixtures/user/user-services-batch-complete-purchasehistory.json');

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

// Adhoc Query endpoints
var explore = {};
explore.queries = require('./fixtures/adhoc-queries/queryList.json');

module.exports = function (nock, gatewayAddr, gatewayPort) {

  /**
   * Set up nock environment. Disable net connection.
   */
  nock.enableNetConnect('cask.co');

  var clientAddr = 'http://' + gatewayAddr + ':' + gatewayPort;
  var options = {allowUnmocked: true};


  /**
   * Adhoc query mocks.
   */
  nock(clientAddr, options)
    .persist()
    .get('/v2/data/explore/queries')
    .reply(200, explore.queries);

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



  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services-info')
    .reply(200, servicesinfo_countcounts);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/PurchaseHistory/services-info')
    .reply(200, servicesinfo_purchasehistory);


  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services')
    .reply(200, apps.CountCounts.services.servicesCompleteDefinition);

  //loadgen service:
  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/loadgen')
    .reply(200, apps.CountCounts.services.loadgen);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/loadgen/runnables/streamEventGen/instances')
    .reply(200, apps.CountCounts.services.loadgen.instances);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/loadgen/status')
    .reply(200, apps.CountCounts.services.loadgen.status);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/loadgen/runtimeargs')
    .reply(200, apps.CountCounts.services.loadgen.runtimeargs);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/loadgen/history')
    .reply(200, apps.CountCounts.services.loadgen.history);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/loadgen/live-info')
    .reply(200, apps.CountCounts.services.loadgen.liveinfo);

  //simpleName service:
  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/simpleName')
    .reply(200, apps.CountCounts.services.loadgen);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/simpleName/runnables/streamEventGen/instances')
    .reply(200, apps.CountCounts.services.loadgen.instances);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/simpleName/status')
    .reply(200, apps.CountCounts.services.loadgen.status);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/simpleName/runtimeargs')
    .reply(200, apps.CountCounts.services.loadgen.runetimeargs);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/simpleName/history')
    .reply(200, apps.CountCounts.services.loadgen.history);

  nock(clientAddr, options)
    .persist()
    .get('/v2/apps/CountCounts/services/simpleName/live-info')
    .reply(200, apps.CountCounts.services.loadgen.liveinfo);


};