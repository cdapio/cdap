/*
 * Copyright © 2020 Cask Data, Inc.
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
import { IWidgetProps } from 'components/AbstractWidget';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { GLOBALS, SCOPES } from 'services/global-constants';
import VersionStore from 'services/VersionStore';
import { objectQuery } from 'services/helpers';
import { MyPipelineApi } from 'api/pipeline';
import CustomSelect from 'components/AbstractWidget/FormInputs/Select';
import T from 'i18n-react';

const PREFIX = 'features.AbstractWidget.PluginListWidget';

interface IPluginListWidgetProps {
  'plugin-type': string;
}

interface IPluginListProps extends IWidgetProps<IPluginListWidgetProps> {}

interface IPlugin {
  name: string;
  type: string;
  description: string;
  className: string;
  artifact: {
    name: string;
    version: string;
    scope: string;
  };
}

const PluginListWidget: React.FC<IPluginListProps> = ({
  value,
  onChange,
  widgetProps,
  disabled,
  dataCy,
}) => {
  const [options, setOptions] = React.useState([]);

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      parentArtifact: GLOBALS.etlDataPipeline,
      version: VersionStore.getState().version,
      extension: objectQuery(widgetProps, 'plugin-type') || 'jdbc',
      scope: SCOPES.SYSTEM,
    };

    MyPipelineApi.getExtensions(params).subscribe((res: IPlugin[]) => {
      if (res.length === 0) {
        const emptyOptions = [
          {
            value: '',
            label: T.translate(`${PREFIX}.emptyLabel`, { pluginType: params.extension }).toString(),
            disabled: true,
          },
        ];
        setOptions(emptyOptions);

        return;
      }

      const displayOptions = res.map((plugin) => {
        return {
          value: plugin.name,
          label: `${plugin.name} (${plugin.artifact.name} ${plugin.artifact.version})`,
        };
      });

      setOptions(displayOptions);
    });
  }, []);

  return (
    <CustomSelect
      value={value}
      onChange={onChange}
      widgetProps={{
        options,
      }}
      disabled={disabled}
      dataCy={dataCy}
    />
  );
};

export default PluginListWidget;
