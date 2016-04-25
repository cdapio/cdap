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

describe('Hydrator++ - Left Panel Actions test', function() {
  var _myPipelineApi,
      _mySettings,
      _GLOBALS,
      _HydratorPlusPlusLeftPanelStore,
      _HydratorPlusPlusPluginActions,
      _$q,
      _$timeout,
      leftPanelStore = {
        plugins: {
          pluginTypes: {},
          pluginToVersionMap: {}
        },
        extensions: []
      },
      extensions = ['batchsource', 'batchsink', 'transform'],
      plugins = [
        { name: 'Stream', artifact: { name: 'core-plugins', version: '1.3.0', scope: 'SYSTEM'}},
        { name: 'Cassandra', artifact: { name: 'cassandra-plugins', version: '1.3.0', scope: 'SYSTEM'}}
      ],
      templates = {
        'default': {
          'cdap-data-pipeline': {
            'batchsource': {
              'NewStream1': Object.assign({}, plugins[0], {pluginName: 'Stream', pluginTemplate: 'NewStream1', properties: {}})
            }
          }
        }
      };
  beforeEach(module('cdap-ui.commons'));
  beforeEach(module('cdap-ui.services'));
  beforeEach(module('cdap-ui.feature.hydratorplusplus'));

  beforeEach(function() {
    try {
      inject(function(HydratorPlusPlusLeftPanelStore, HydratorPlusPlusPluginActions, myPipelineApi, $q, $timeout, mySettings) {
        _HydratorPlusPlusLeftPanelStore = HydratorPlusPlusLeftPanelStore;
        _HydratorPlusPlusPluginActions = HydratorPlusPlusPluginActions;
        _myPipelineApi = myPipelineApi;
        _$q = $q;
        _mySettings = mySettings;
        _$timeout = $timeout;

        spyOn(_myPipelineApi, 'fetchExtensions').andCallFake(function(re) {
          return {
            $promise: {
              then: function(cb, errCb) {
                cb(extensions);
              }
            }
          };
        });

        spyOn(_myPipelineApi, 'fetchPlugins').andCallFake(function(re) {
          return {
            $promise: {
              then: function(cb, errCb) {
                _$timeout(function() {
                  cb(plugins);
                });
              }
            }
          };
          _$timeout.flush();
        });

        spyOn(_mySettings, 'get').andCallFake(function(resource) {
          if (resource === 'pluginTemplates') {
            return {
              then: function(cb, errCb) {
                _$timeout(function() {
                  cb(templates);
                });
              }
            };
          } else if (resource === 'plugin-default-version') {
            return {
              then: function(cb, errCb) {
                _$timeout(function() {
                  cb(defaultVersionMap);
                });
              }
            };
          }
          _$timeout.flush();
        });

      });
    } catch(e) {
      console.log(e);
    }
  });

  it('Should fetch extensions and update the store', function() {
    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _extensions = _HydratorPlusPlusLeftPanelStore.getState().extensions;
      expect(_extensions).toEqual(extensions);
    });

    _HydratorPlusPlusLeftPanelStore.dispatch(
      _HydratorPlusPlusPluginActions.fetchExtensions({
        namespace: 'default',
        pipelineType: 'cdap-etl-batch',
        version: '3.4',
        scope: 'SYSTEM'
      })
    );
  });

  it('Should fetch plugins and update the store', function() {
    _HydratorPlusPlusLeftPanelStore.dispatch(
      _HydratorPlusPlusPluginActions.fetchExtensions({
        namespace: 'default',
        pipelineType: 'cdap-etl-batch',
        version: '3.4.0',
        scope: 'SYSTEM'
      })
    );

    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var _plugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      expect(_plugins).toEqual(plugins);
    });

    _HydratorPlusPlusLeftPanelStore.dispatch(
      _HydratorPlusPlusPluginActions.fetchPlugins(extensions[0], {
        namespace: 'default',
        pipelineType: 'cdap-etl-batch',
        version: '3.4.0',
        extensionType: 'batchsource'
      })
    );
    sub();
  });

  it('Should fetch templates and update the store', function() {
    var sub = _HydratorPlusPlusLeftPanelStore.subscribe(function() {
      var plugins = _HydratorPlusPlusLeftPanelStore.getState().plugins.pluginTypes[extensions[0]];
      var template = plugins.filter( plug => plug.pluginTemplate);
      if (template.length) {
        template = template[0];
      }
      expect(template.pluginTemplate).toBe('NewStream1');
      expect(template.type).toBe(extensions[0]);
      expect(template.pluginName).toBe('Stream');
    });

    _HydratorPlusPlusLeftPanelStore.dispatch(
      _HydratorPlusPlusPluginActions.fetchTemplates(
        { namespace: 'default' },
        { namespace: 'default', pipelineType: 'cdap-data-pipeline' }
      )
    );

  });

});
