angular.module(PKG.name + '.services')
  .factory('myWorkFlowApi', function($state, myCdapUrl, $resource, myAuth, MY_CONFIG) {

    var url = myCdapUrl.constructUrl,
        schedulepath = '/apps/:appId/schedules/:scheduleId',
        basepath = '/apps/:appId/workflows/:workflowId';

    function getConfig (method, type, path, isArray) {
      var config = {
        url: url({ _cdapNsPath: path }),
        method: method,
        options: { type: type}
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
      pollRunDetail: getConfig('GET', 'POLL', basepath + '/runs/:runId'),
      logs: getConfig('GET', 'REQUEST', basepath + '/runs/:runId/logs/next', true),
      schedules: getConfig('GET', 'REQUEST', basepath + '/schedules', true),
      schedulesPreviousRunTime: getConfig('GET', 'REQUEST', basepath + '/previousruntime', true),
      pollScheduleStatus: getConfig('GET', 'POLL', schedulepath + '/status'),
      scheduleSuspend: getConfig('POST', 'REQUEST', schedulepath + '/suspend'),
      scheduleResume: getConfig('POST', 'REQUEST', schedulepath + '/resume'),
      getCurrent: getConfig('GET', 'REQUEST', basepath + '/:runid/current', true)
    });
  });
