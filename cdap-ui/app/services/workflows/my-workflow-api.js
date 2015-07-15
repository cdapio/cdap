angular.module(PKG.name + '.services')
  .factory('myWorkFlowApi', function($state, myCdapUrl, $resource) {

    var url = myCdapUrl.constructUrl,
        schedulepath = '/apps/:appId/schedules/:scheduleId',
        basepath = '/apps/:appId/workflows/:workflowId';

    function getConfig (method, type, path, isArray, interval) {
      var config = {
        url: url({ _cdapNsPath: path }),
        method: method,
        options: {
          type: type,
          interval: interval || 10000
        }
      };
      if (isArray) {
        config.isArray = true;
      }
      return config;
    }

    return $resource(
      url({ _cdapNsPath: basepath }),
    {
      appId: '@appId',
      workflowId: '@workflowId',
      scheduleId: '@scheduleId',
      runId: '@runId'
    },
    {
      get: getConfig('GET', 'REQUEST', basepath),
      status: getConfig('GET', 'REQUEST', basepath + '/status'),
      start: getConfig('POST', 'REQUEST', basepath + '/start'),
      stop: getConfig('POST', 'REQUEST', basepath + '/stop'),
      pollStatus: getConfig('GET', 'POLL', basepath + '/status'),

      runs: getConfig('GET', 'REQUEST', basepath + '/runs', true),
      runDetail: getConfig('GET', 'REQUEST', basepath + '/runs/:runId'),
      pollRuns: getConfig('GET', 'POLL', basepath + '/runs', true),
      pollRunDetail: getConfig('GET', 'POLL', basepath + '/runs/:runId', false, 1000),
      stopPollRunDetail: getConfig('GET', 'POLL-STOP', basepath + '/runs/:runId'),
      stopRun: getConfig('POST', 'REQUEST', basepath + '/runs/:runId/stop'),
      suspendRun: getConfig('POST', 'REQUEST', basepath + '/runs/:runId/suspend'),
      resumeRun: getConfig('POST', 'REQUEST', basepath + '/runs/:runId/resume'),

      nextLogs: getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/next', true),
      prevLogs: getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/prev', true),

      schedules: getConfig('GET', 'REQUEST', basepath + '/schedules', true),
      schedulesPreviousRunTime: getConfig('GET', 'REQUEST', basepath + '/previousruntime', true),
      pollScheduleStatus: getConfig('GET', 'POLL', schedulepath + '/status', false, 2000),
      scheduleSuspend: getConfig('POST', 'REQUEST', schedulepath + '/suspend'),
      scheduleResume: getConfig('POST', 'REQUEST', schedulepath + '/resume'),
      getCurrent: getConfig('GET', 'REQUEST', basepath + '/:runid/current', true)
    });
  });
