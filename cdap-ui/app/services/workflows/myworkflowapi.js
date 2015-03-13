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
        options: { type: 'POLL', id: runsId },
        user: myAuth.currentUser || null,
        headers: {
          authorization: (myAuth.currentUser.token ? 'Bearer ' + myAuth.currentUser.token: null)
        },
        interceptor: {
          response: function(response) {
            return response;
          }
        }
      },
      runsStop: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        method: 'GET',
        options: { type: 'POLL-STOP', id: runsId },
        user: myAuth.currentUser || null,
        headers: {
          authorization: (myAuth.currentUser.token ? 'Bearer ' + myAuth.currentUser.token: null)
        },
        interceptor: {
          response: function(response) {
            return response;
          }
        }
      },
      status: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'POLL', id: statusId },
        user: myAuth.currentUser || null,
        headers: {
          authorization: (myAuth.currentUser.token ? 'Bearer ' + myAuth.currentUser.token: null)
        },
        interceptor: {
          response: function(response) {
            return response;
          }
        }
      },
      statusStop: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'POLL-STOP', id: statusId },
        user: myAuth.currentUser || null,
        headers: {
          authorization: (myAuth.currentUser.token ? 'Bearer ' + myAuth.currentUser.token: null)
        },
        interceptor: {
          response: function(response) {
            return response;
          }
        }
      },
      start: {
        url: url({ _cdapNsPath: basepath + '/start' }),
        method: 'POST',
        options: { type: 'REQUEST' },
        user: myAuth.currentUser || null,
        headers: {
          authorization: (myAuth.currentUser.token ? 'Bearer ' + myAuth.currentUser.token: null)
        },
        interceptor: {
          response: function(response) {
            return response;
          }
        }
      },
      stop: {
        url: url({ _cdapNsPath: basepath + '/stop' }),
        method: 'POST',
        options: { type: 'REQUEST' },
        user: myAuth.currentUser || null,
        headers: {
          authorization: (myAuth.currentUser.token ? 'Bearer ' + myAuth.currentUser.token: null)
        },
        interceptor: {
          response: function(response) {
            return response;
          }
        }
      }
    });
  });
