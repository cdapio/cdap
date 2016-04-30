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

function HydratorPlusPlusOrderingFactory() {
  function getArtifactDisplayName (artifactName) {
    let artifactMap = {
      'cdap-etl-batch': 'ETL Batch',
      'cdap-etl-realtime': 'ETL Realtime',
      'cdap-data-pipeline': 'Data Pipeline (Beta)'
    };

    return artifactMap[artifactName];
  }

  function getPluginTypeDisplayName (pluginType) {
    let pluginTypeMap = {
      'transform': 'Transform',
      'batchsource': 'Source',
      'batchsink': 'Sink',
      'batchaggregator': 'Aggregate',
      'realtimesink': 'Sink',
      'realtimesource': 'Source',
      'sparksink': 'Model',
      'sparkcompute': 'Compute'
    };

    return pluginTypeMap[pluginType];
  }

  function orderPluginTypes (pluginsMap) {
    if (!pluginsMap.length) {
      return pluginsMap;
    }
    let orderedTypes = [];

    let source = pluginsMap.filter( p => { return p.name === 'Source'; });
    let transform = pluginsMap.filter( p => { return p.name === 'Transform'; });
    let sink = pluginsMap.filter( p => { return p.name === 'Sink'; });
    let aggregator = pluginsMap.filter( p => { return p.name === 'Aggregate'; });
    let sparksink = pluginsMap.filter( p => { return p.name === 'Model'; });
    let sparkcompute = pluginsMap.filter( p => { return p.name === 'Compute'; });

    orderedTypes.push(source[0]);
    orderedTypes.push(transform[0]);
    orderedTypes.push(sink[0]);
    if (aggregator.length) {
      orderedTypes.push(aggregator[0]);
    }
    if (sparkcompute.length) {
      orderedTypes.push(sparkcompute[0]);
    }
    if (sparksink.length) {
      orderedTypes.push(sparksink[0]);
    }

    // Doing this so that the SidePanel does not lose the reference of the original
    // array object.
    angular.forEach(orderedTypes, (type, index) => {
      pluginsMap[index] = type;
    });

    return pluginsMap;
  }

  return {
    getArtifactDisplayName: getArtifactDisplayName,
    getPluginTypeDisplayName: getPluginTypeDisplayName,
    orderPluginTypes: orderPluginTypes
  };
}

angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .service('HydratorPlusPlusOrderingFactory', HydratorPlusPlusOrderingFactory);
