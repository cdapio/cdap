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

'use strict';
describe('Hydrator++ - Left Panel Store tests', function() {
  var _LEFTPANELSTORE_ACTIONS,
      _Redux,
      _ReduxThunk,
      _GLOBALS,
      _DAGPlusPlusFactory,
      _myHelpers,
      _HydratorPlusPlusLeftPanelStore,
      _MyCDAPDataSource,
      _leftPanelStore = {
        plugins: {
          pluginTypes: {},
          pluginToVersionMap: {}
        },
        extensions: []
      },
      extensions = ['batchsource', 'batchsink', 'transform'],
      pluginToVersionMap = {
        'Stream-batchsource-core-plugins': {
          name: 'core-plugins',
          version: '1.5.1',
          scope: 'SYSTEM'
        }
      },
      batchSourcePlugins = [
        { name: 'Stream', artifact: { name: 'core-plugins', version: '1.3.0', scope: 'SYSTEM' } },
        { name: 'Stream', artifact: { name: 'core-plugins', version: '1.4.0', scope: 'SYSTEM' } },
        { name: 'Stream', artifact: { name: 'core-plugins', version: '1.5.0', scope: 'SYSTEM' } },
        { name: 'Stream', artifact: { name: 'core-plugins', version: '1.5.1', scope: 'SYSTEM' } },
        { name: 'Cassandra', artifact: { name: 'cassandra-plugins', version: '1.3.0', scope: 'SYSTEM' } }
      ],
      batchSinkPlugins = [
        { name: 'Table', artifact: { name: 'core-plugins', version: '1.3.0', scope: 'SYSTEM' } },
        { name: 'Hive', artifact: { name: 'core-plugins', version: '1.3.0', scope: 'SYSTEM' } }
      ],
      templatesMap  = {
        'NewElasticSearch': {
          artifact: {name: 'elasticsearch-plugins', version: '1.3.0', scope: 'SYSTEM'},
          pluginName: 'ElasticSearch',
          pluginType: 'batchsource',
          pluginTemplate: 'NewElasticSearch',
          properties: {'es.host': 'host'}
        }
      },
      templates = {
        'default': {
          'cdap-etl-batch': {
            batchsource: templatesMap
          }
        }
      };
  beforeEach(module('cdap-ui.commons'));
  beforeEach(module('cdap-ui.services'));
  beforeEach(module('cdap-ui.feature.hydratorplusplus'));

  beforeEach(function() {
    try {
      inject(function(LEFTPANELSTORE_ACTIONS, Redux, ReduxThunk, GLOBALS, DAGPlusPlusFactory, myHelpers, HydratorPlusPlusLeftPanelStore, MyCDAPDataSource) {
        _LEFTPANELSTORE_ACTIONS = LEFTPANELSTORE_ACTIONS;
        _Redux = Redux;
        _ReduxThunk = ReduxThunk;
        _GLOBALS = GLOBALS;
        _DAGPlusPlusFactory = DAGPlusPlusFactory;
        _myHelpers = myHelpers;
        _HydratorPlusPlusLeftPanelStore = HydratorPlusPlusLeftPanelStore;
        _MyCDAPDataSource = new MyCDAPDataSource();

        spyOn(_MyCDAPDataSource, 'request').andCallFake(function(re) {
          console.log('Something requested');
          return 'something';
        });
      });
    } catch(e){
      console.log(e);
    }
  });

  it('Should have a default value', function() {
    expect(_HydratorPlusPlusLeftPanelStore.getState()).toEqual(_leftPanelStore);
  });

  it('Should be possible to add different plugin types', function() {
    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _extensions = _HydratorPlusPlusLeftPanelStore.getState().extensions;
      expect(_extensions).toEqual(extensions);
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.EXTENSIONS_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', extensions: extensions })
    });
    sub();
  });

  it('Should be possible to add a pluginToVersionMap object to the store', function() {
    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _pluginToVersionMap = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginToVersionMap;
      expect(_pluginToVersionMap).toEqual(pluginToVersionMap);
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGINS_DEFAULT_VERSION_FETCH,
      payload: {res: pluginToVersionMap}
    });
    sub();
  });

  it('Should be possible to add plugins under each plugin Type', function() {

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.EXTENSIONS_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', extensions: extensions })
    });

    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _plugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      expect(_plugins.length).toEqual(2);
      expect(_plugins[0].name).toEqual(batchSourcePlugins[0].name);
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGINS_FETCH,
      payload: Object.assign({}, {plugins: batchSourcePlugins, extension: extensions[0]})
    });

    sub();
  });

  it('Should be possible to add a template under a plugin type', function() {
    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.EXTENSIONS_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', extensions: extensions })
    });

    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _plugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      expect(_plugins.length > 0).toBeTruthy();
      expect(_plugins[0].pluginName).toBe(templatesMap.NewElasticSearch.pluginName);
      expect(_plugins[0].name).toBe(templatesMap.NewElasticSearch.pluginTemplate);
    });
    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGIN_TEMPLATE_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', namespace: 'default', res: templates})
    });
    sub();
  });

  it('Should handle async plugins fetch + templates fetch', function() {
    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.EXTENSIONS_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', extensions: extensions })
    });

    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _plugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      expect(_plugins.length).toEqual(2);
      expect(_plugins[0].name).toEqual(batchSourcePlugins[0].name);
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGINS_FETCH,
      payload: Object.assign({}, {plugins: batchSourcePlugins, extension: extensions[0]})
    });
    sub();

    sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _batchSourcePlugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      var _templates = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]].filter(plug => plug.pluginTemplate);
      expect(_batchSourcePlugins.length).toBe(3);
      expect(_templates.length).toBe(1);
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGIN_TEMPLATE_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', namespace: 'default', res: templates})
    });

    sub();

    sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _templates = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]].filter(plug => plug.pluginTemplate);
      var _batchSourcePlugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      var _batchSinkPlugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[1]];
      expect(_templates.length).toBe(1);
      expect(_batchSourcePlugins.length + _batchSinkPlugins.length).toBe(5); // plugins + one template we added above.
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGINS_FETCH,
      payload: Object.assign({}, {plugins: batchSinkPlugins, extension: extensions[1]})
    });
  });

  it('Should handle async plugins fetch + default plugin version from user store', function() {
    var sub;
    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGINS_DEFAULT_VERSION_FETCH,
      payload: {res: pluginToVersionMap}
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGINS_FETCH,
      payload: Object.assign({}, {plugins: batchSourcePlugins, extension: extensions[0]})
    });

    sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _plugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      expect(_plugins.length).toBe(3);
      var streamPlugin = _plugins.filter( plug => plug.name === 'Stream');
      if (streamPlugin.length) {
        expect(streamPlugin[0].defaultArtifact.version).toBe('1.5.1');
      }
    });
    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGIN_TEMPLATE_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', namespace: 'default', res: templates})
    });

  });

  it('Should reset to default value on \'RESET\' ', function() {

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.EXTENSIONS_FETCH,
      payload: Object.assign({}, { pipelineType: 'cdap-etl-batch', extensions: extensions })
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({
      type: _LEFTPANELSTORE_ACTIONS.PLUGINS_FETCH,
      payload: Object.assign({}, {plugins: batchSourcePlugins, extension: extensions[0]})
    });

    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      expect(_HydratorPlusPlusLeftPanelStore.getState()).toEqual(_leftPanelStore)
    });

    _HydratorPlusPlusLeftPanelStore.dispatch({ type: _LEFTPANELSTORE_ACTIONS.RESET });
    sub();
  });

});
