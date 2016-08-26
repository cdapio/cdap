/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

function myTrackerApi(myCdapUrl, $resource, myAuth, myHelpers, CDAP_UI_CONFIG) {
  var url = myCdapUrl.constructUrl,
      searchPath = '/namespaces/:namespace/metadata/search?target=stream&target=dataset&target=view',
      basePath = '/namespaces/:namespace/:entityType/:entityId',
      programPath = '/namespaces/:namespace/apps/:appId/:programType/:programId/runs/:runId',
      auditPath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId + '/services/' + CDAP_UI_CONFIG.tracker.serviceId + '/methods/v1/auditlog/:entityType/:entityId',
      navigatorPath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.navigator.appId,
      trackerApp = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId,
      propertyPath = '/namespaces/:namespace/:entityType/:entityId/metadata/properties',
      // FIXME: This service name needs to come from CDAP_UI_CONFIG. Need to figure out how to do this.
      topEntitiesPath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId + '/services/' + CDAP_UI_CONFIG.tracker.serviceId + '/methods/v1/auditmetrics/top-entities/:entity',
      auditHistogramPath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId + '/services/' + CDAP_UI_CONFIG.tracker.serviceId + '/methods/v1/auditmetrics/audit-histogram',
      timeSincePath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId + '/services/' + CDAP_UI_CONFIG.tracker.serviceId + '/methods/v1/auditmetrics/time-since',
      exploreQueryPath = '/namespaces/:namespace/data/explore/queries',
      baseQueryPath = '/data/explore/queries/:handle',
      truthMeterPath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId + '/services/' + CDAP_UI_CONFIG.tracker.serviceId + '/methods/v1/tracker-meter',
      tagsPath = '/namespaces/:namespace/apps/' + CDAP_UI_CONFIG.tracker.appId + '/services/' + CDAP_UI_CONFIG.tracker.serviceId + '/methods/v1/tags';

  return $resource(
    url({ _cdapPath: searchPath }),
  {
    namespace: '@namespace'
  },
  {
    search: myHelpers.getConfig('GET', 'REQUEST', searchPath, true),
    properties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata', true),
    viewsProperties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/views/:viewId/metadata', true),
    getLineage: myHelpers.getConfig('GET', 'REQUEST', basePath + '/lineage?collapse=access&collapse=run&collapse=component'),
    getProgramRunStatus: myHelpers.getConfig('GET', 'REQUEST', programPath),
    getAuditLogs: myHelpers.getConfig('GET', 'REQUEST', auditPath, false, { suppressErrors: true }),
    getStreamProperties: myHelpers.getConfig('GET', 'REQUEST', '/namespaces/:namespace/streams/:entityId'),
    getDatasetSystemProperties: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata/properties?scope=SYSTEM', false, { suppressErrors: true }),
    getSystemTags: myHelpers.getConfig('GET', 'REQUEST', basePath + '/metadata/tags?scope=SYSTEM', true),
    getDatasetDetail: myHelpers.getConfig('GET', 'REQUEST', '/namespaces/:namespace/data/datasets/:entityId'),
    deployNavigator: myHelpers.getConfig('PUT', 'REQUEST', navigatorPath, false, { contentType: 'application/json' }),
    getCDAPConfig: myHelpers.getConfig('GET', 'REQUEST', '/config/cdap', true),
    getNavigatorApp: myHelpers.getConfig('GET', 'REQUEST', navigatorPath, false, { suppressErrors: true }),
    getNavigatorStatus: myHelpers.getConfig('GET', 'REQUEST', navigatorPath + '/flows/MetadataFlow/runs', true),
    toggleNavigator: myHelpers.getConfig('POST', 'REQUEST', navigatorPath + '/flows/MetadataFlow/:action', false),
    getTrackerApp: myHelpers.getConfig('GET', 'REQUEST', trackerApp, false, { suppressErrors: true }),
    deployTrackerApp: myHelpers.getConfig('PUT', 'REQUEST', trackerApp),
    stopMultiplePrograms: myHelpers.getConfig('POST', 'REQUEST', '/namespaces/:namespace/stop', true, { suppressErrors: true }),
    startTrackerProgram: myHelpers.getConfig('POST', 'REQUEST', trackerApp + '/:programType/:programId/start', false, { suppressErrors: true }),
    trackerProgramStatus: myHelpers.getConfig('GET', 'REQUEST', trackerApp + '/:programType/:programId/status', false, { suppressErrors: true }),
    getTopEntities: myHelpers.getConfig('GET', 'REQUEST', topEntitiesPath, true, { suppressErrors: true }),
    getAuditHistogram: myHelpers.getConfig('GET', 'REQUEST', auditHistogramPath, false, { suppressErrors: true }),
    getTimeSince: myHelpers.getConfig('GET', 'REQUEST', timeSincePath, false, { suppressErrors: true }),
    getTruthMeter: myHelpers.getConfig('POST', 'REQUEST', truthMeterPath, false, { suppressErrors: true }),

    // USER AND PREFERRED TAGS
    getTags: myHelpers.getConfig('GET', 'REQUEST', tagsPath, false, { suppressErrors: true }),
    demotePreferredTags: myHelpers.getConfig('POST', 'REQUEST', tagsPath + '/demote', false, { suppressErrors: true }),
    promoteUserTags: myHelpers.getConfig('POST', 'REQUEST', tagsPath + '/promote', false, { suppressErrors: true }),
    validatePreferredTags: myHelpers.getConfig('POST', 'REQUEST', tagsPath + '/validate', false, { suppressErrors: true }),
    getEntityTags: myHelpers.getConfig('GET', 'REQUEST', tagsPath + '/:entityType/:entityId', false, { suppressErrors: true }),
    addEntityTag: myHelpers.getConfig('POST', 'REQUEST', tagsPath + '/promote/:entityType/:entityId', false, { suppressErrors: true }),
    deletePreferredTags: myHelpers.getConfig('DELETE', 'REQUEST', tagsPath + '/preferred', false, { suppressErrors: true }),
    deleteEntityTag: myHelpers.getConfig('DELETE', 'REQUEST', tagsPath + '/delete/:entityType/:entityId', false, { suppressErrors: true }),

    // METADATA PROPERTIES CONTROL
    deleteEntityProperty: myHelpers.getConfig('DELETE', 'REQUEST', propertyPath + '/:key', false, { suppressErrors: true }),
    addEntityProperty: myHelpers.getConfig('POST', 'REQUEST', propertyPath, false, { suppressErrors: true }),

    // EXPLORE QUERY
    postQuery: myHelpers.getConfig('POST', 'REQUEST', exploreQueryPath),
    getQueryResults: myHelpers.getConfig('POST', 'REQUEST', baseQueryPath + '/next', true),
    getQuerySchema: myHelpers.getConfig('GET', 'REQUEST', baseQueryPath + '/schema', true),
  });
}

angular.module(PKG.name + '.feature.tracker')
  .factory('myTrackerApi', myTrackerApi);
