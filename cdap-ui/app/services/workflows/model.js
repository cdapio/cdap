angular.module(PKG.name + '.services')
  .factory('WorkflowsFactory', function(MySocketProvider, $state, myCdapUrl) {


    return MySocketProvider(
      myCdapUrl.constructUrl({
        _cdapNsPath: '/apps/:appId/workflows/:workflowId'
      }),
    {
      appId: '@appId',
      workflowId: '@workflowId'
    },
    {
      runs: {
        url: myCdapUrl.constructUrl({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/runs'
        }),
        method: 'GET',
        params: {
          type: 'poll'
        }
      },
      getStatus: {
        url: myCdapUrl.constructUrl({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/status'
        }),
        method: 'GET',
        params: {
          type: 'poll'
        }
      },
      start: {
        url: myCdapUrl.constructUrl({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/start'
        }),
        method: 'POST'
      },
      stop: {
        url: myCdapUrl.constructUrl({
          _cdapNsPath: '/apps/:appId/workflows/:workflowId/stop'
        }),
        method: 'POST'
      }
    });
  });
