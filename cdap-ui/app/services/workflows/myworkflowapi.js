angular.module(PKG.name + '.services')
  .factory('myWorkFlowApi', function(MySocketProvider, $state, myCdapUrl) {

    var url = myCdapUrl.constructUrl,
        basepath = '/apps/:appId/workflows/:workflowId';

    return MySocketProvider(
      url({ _cdapNsPath: basepath }),
    {
      appId: '@appId',
      workflowId: '@workflowId'
    },
    {
      runs: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        method: 'GET',
        options: { type: 'poll' }
      },
      status: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'poll' }
      },
      start: {
        url: url({ _cdapNsPath: basepath + '/start' }),
        method: 'POST'
      },
      stop: {
        url: url({ _cdapNsPath: basepath + '/stop' }),
        method: 'POST'
      }
    });
  });
