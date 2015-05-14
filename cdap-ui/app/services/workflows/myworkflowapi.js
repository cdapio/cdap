angular.module(PKG.name + '.services')
  .factory('myWorkFlowApi', function($state, myCdapUrl, $resource, myAuth, MY_CONFIG) {

    var url = myCdapUrl.constructUrl,
        schedulepath = '/apps/:appId/schedules/:scheduleId',
        basepath = '/apps/:appId/workflows/:workflowId';

    return $resource(
      url({ _cdapNsPath: basepath }),
    {
      appId: '@appId',
      workflowId: '@workflowId',
      scheduleId: '@scheduleId',
      runId: '@runId'
    },
    {
      get: {
        url: url({ _cdapNsPath: basepath }),
        method: 'GET',
        options: { type: 'REQUEST' }
      },

      // Status, Start & Stop of a workflow.
      status: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'REQUEST' }
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
      },

      pollStatus: {
        url: url({ _cdapNsPath: basepath + '/status' }),
        method: 'GET',
        options: { type: 'POLL' }
      },

      // Runs and runDetail of a workflow
      runs: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        method: 'GET',
        isArray: true,
        options: { type: 'REQUEST'}
      },
      runDetail: {
        url: url({ _cdapNsPath: basepath + '/runs/:runId'}),
        method: 'GET',
        options: { type: 'REQUEST'}
      },
      pollRuns: {
        url: url({ _cdapNsPath: basepath + '/runs'}),
        method: 'GET',
        isArray: true,
        options: { type: 'POLL'}
      },
      pollRunDetail: {
        url: url({ _cdapNsPath: basepath + '/runs/:runId'}),
        method: 'GET',
        options: { type: 'POLL'}
      },

      logs: {
        url: url({_cdapNsPath: basepath + '/runs/:runId/logs/next'}),
        method: 'GET',
        isArray: true,
        options: {type: 'REQUEST'}
      },

      schedules: {
        url: url({_cdapNsPath: basepath + '/schedules'}),
        method: 'GET',
        isArray: true,
        options: {type: 'REQUEST'}
      },

      schedulesPreviousRunTime: {
        url: url({_cdapNsPath: basepath + '/previousruntime'}),
        method: 'GET',
        isArray: true,
        options: {type: 'REQUEST'}
      },

      pollScheduleStatus: {
        url: url({_cdapNsPath: schedulepath + '/status'}),
        method: 'GET',
        options: {type: 'POLL'}
      },
      scheduleSuspend: {
        url: url({_cdapNsPath: schedulepath + '/suspend'}),
        method: 'POST',
        options: {type: 'REQUEST'}
      },
      scheduleResume: {
        url: url({_cdapNsPath: schedulepath + '/resume'}),
        method: 'POST',
        options: {type: 'REQUEST'}
      }

    });
  });
