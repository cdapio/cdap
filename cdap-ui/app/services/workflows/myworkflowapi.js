angular.module(PKG.name + '.services')
  .factory('myWorkFlowApi', function(MySocketProvider, $state, myCdapUrl) {

    var url = myCdapUrl.constructUrl;

    return MySocketProvider(
      url({
        _cdapNsPath: '/apps/:appId/workflows/:workflowId'
      }),
    {
      appId: '@appId',
      workflowId: '@workflowId'
    },
    {
      runs: {
        url: url({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/runs'
        }),
        method: 'GET',
        options: {
          type: 'poll'
        }
      },
      status: {
        url: url({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/status'
        }),
        method: 'GET',
        options: {
          type: 'poll'
        }
      },
      start: {
        url: url({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/start'
        }),
        method: 'POST'
      },
      stop: {
        url: url({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/stop'
        }),
        method: 'POST'
      }
    });
  });
