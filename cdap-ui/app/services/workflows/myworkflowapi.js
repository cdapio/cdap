angular.module(PKG.name + '.services')
  .factory('myWorkFlowApi', function($state, myCdapUrl, $resource, myAuth, MY_CONFIG) {

    var url = myCdapUrl.constructUrl,
        basepath = '/apps/:appId/workflows/:workflowId';
    var runsId = Date.now(),
        statusId = Date.now() + 1;

    return $resource(
      url({ _cdapNsPath: basepath }),
    {
      appId: '@appId',
      workflowId: '@workflowId'
    },
    {
      runs: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        method: 'GET',
        isArray: true,
        options: { type: 'POLL', id: runsId }
      },
      runsStop: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        isArray: true,
        method: 'GET',
        options: { type: 'POLL-STOP', id: runsId }
      },
      status: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'POLL', id: statusId }
      },
      statusStop: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'POLL-STOP', id: statusId }
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
