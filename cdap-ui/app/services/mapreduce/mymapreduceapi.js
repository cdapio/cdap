angular.module(PKG.name + '.services')
  .factory('myMapreduceApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace/apps/:appId/mapreduce/:mapreduceId';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      appId: '@appId',
      mapreduceId: '@mapreduceId',
      runId: '@runId'
    },
    {
      get: myHelpers.getConfig('GET', 'REQUEST', basepath),
      runs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs', true),
      logs: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/next', true),
      pollInfo: myHelpers.getConfig('GET', 'POLL', basepath + '/runs/:runId/info'),
      runDetail: myHelpers.getConfig('GET', 'REQUEST', basepath + '/runs/:runId')
    });
  });
