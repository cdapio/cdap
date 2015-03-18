angular.module(PKG.name + '.services')
  .factory('myWorkFlowApi', function($state, myCdapUrl, $resource, myAuth, MY_CONFIG) {

    var url = myCdapUrl.constructUrl,
        basepath = '/apps/:appId/workflows/:workflowId';

    return $resource(
      url({ _cdapNsPath: basepath }),
    {
      appId: '@appId',
      workflowId: '@workflowId'
    },
    {
      get: {
        url: url({ _cdapNsPath: basepath }),
        method: 'GET',
        options: { type: 'REQUEST' }
      },
      runs: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        method: 'GET',
        isArray: true,
        options: { type: 'POLL'}
      },
      runsStop: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        isArray: true,
        method: 'GET',
        options: { type: 'POLL-STOP' }
      },
      status: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'POLL' }
      },
      statusStop: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'POLL-STOP' }
      },
      start: {
        url: url({ _cdapNsPath: basepath + '/start' }),
        method: 'POST',
        options: { type: 'REQUEST' }
      },
      stop: {
        url: url({ _cdapNsPath: basepath + '/stop' }),
        method: 'POST',
        options: { type: 'REQUEST' }
      }
    });
  });
