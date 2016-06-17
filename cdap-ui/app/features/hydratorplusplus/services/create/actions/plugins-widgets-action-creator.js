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

class HydratorPlusPlusPluginsWidgetsActions {
  constructor(PLUGINS_WIDGETS_ACTIONS, myPipelineApi) {
    this.pluginsWidgetsActions = PLUGINS_WIDGETS_ACTIONS;
    this.api = myPipelineApi;
  }
  fetchWidgetJson(namespace, artifact) {
    let artifactName, artifactVersion, scope;
    artifactName = artifact.name;
    artifactVersion = artifact.version;
    scope = artifact.scope;
    
    let key = `${artifactName}-${artifactVersion}-${scope}`;

    return (dispatch) => {
      dispatch({
        type: this.pluginsWidgetsActions.WIDGET_JSON_FETCH_START,
        payload: { key }
      });

      this.api
          .fetchArtifactProperties({
            namespace,
            artifactName,
            artifactVersion,
            scope
          })
          .$promise
          .then( res => {
            dispatch({
              type: this.pluginsWidgetsActions.WIDGET_JSON_FETCH,
              payload: { key, res }
            });
          });
    };
  }
}

HydratorPlusPlusPluginsWidgetsActions.$inject = ['PLUGINS_WIDGETS_ACTIONS', 'myPipelineApi'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .service('HydratorPlusPlusPluginsWidgetsActions', HydratorPlusPlusPluginsWidgetsActions);
