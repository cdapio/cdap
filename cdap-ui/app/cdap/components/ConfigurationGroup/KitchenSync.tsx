/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import ConfigurationGroupView from 'components/ConfigurationGroup';
import { MyPipelineApi } from 'api/pipeline';

/**
 * This file is being used as a playground. Will be removed once the configuration group work is complete.
 */
const ConfigurationGroupKitchenSync: React.FC = () => {
  const [pluginProperties, setPluginProperties] = React.useState();
  const [widgetJson, setWidgetJson] = React.useState();
  const [values, setValues] = React.useState({});

  React.useEffect(() => {
    // Fetch plugin properties and widget json
    const pluginParams = {
      namespace: 'default',
      parentArtifact: 'cdap-data-pipeline',
      version: '6.1.0-SNAPSHOT',
      extension: 'batchsource',
      pluginName: 'File',
      scope: 'SYSTEM',
      artifactName: 'core-plugins',
      artifactScope: 'SYSTEM',
      limit: 1,
      order: 'DESC',
    };

    MyPipelineApi.getPluginProperties(pluginParams).subscribe((res) => {
      setPluginProperties(res[0].properties);
    });
  }, []);

  React.useEffect(() => {
    const widgetKey = `widgets.File-batchsource`;
    const widgetParams = {
      namespace: 'default',
      artifactName: 'core-plugins',
      scope: 'SYSTEM',
      artifactVersion: '2.3.0-SNAPSHOT',
      keys: widgetKey,
    };
    MyPipelineApi.fetchWidgetJson(widgetParams).subscribe((res) => {
      const parsedWidget = JSON.parse(res[widgetKey]);
      setWidgetJson(parsedWidget);
    });
  }, []);

  return (
    <div className="container">
      <h1>Kitchen Sync</h1>
      <pre style={{ border: '2px solid #bbbbbb', padding: '15px' }}>
        {JSON.stringify(values, null, 2)}
      </pre>

      <ConfigurationGroupView
        values={values}
        onChange={setValues}
        pluginProperties={pluginProperties}
        widgetJson={widgetJson}
      />
    </div>
  );
};

export default ConfigurationGroupKitchenSync;
