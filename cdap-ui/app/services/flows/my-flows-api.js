angular.module(PKG.name + '.services')
  .factory('myFlowsApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace/apps/:appId/flows/:flowId';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      appId: '@appId',
      flowId: '@flowId',
      runId: '@runId',
      flowletId: '@flowletId'
    },
    {
      get: myHelpers.getConfig('GET', 'REQUEST', basepath),
      runs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs', true),
      logs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/next', true),
      getFlowletInstance: myHelpers.getConfig('GET', 'REQUEST', basepath + '/flowlets/:flowletId/instances'),
      pollFlowletInstance: myHelpers.getConfig('GET', 'POLL', basepath + '/flowlets/:flowletId/instances'),
      setFlowletInstance: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/flowlets/:flowletId/instances')

    });
  });
