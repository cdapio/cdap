/**
 * Http mock injector for simulating responses from API.
 */
var system = {};
system.services = {};
system.services.statusIncomplete = require('./fixtures/system/services/status_incomplete.json');
system.services.statusComplete = require('./fixtures/system/services/status_complete.json');


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
    .get('/v2/system/services/status')
    .times(4)
    .reply(200, system.services.statusIncomplete);

  nock(clientAddr, options)
    .persist()
    .get('/v2/system/services/status')
    .reply(200, system.services.statusComplete);

};