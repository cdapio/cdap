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

import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { useOutputState } from 'components/PluginJSONCreator/Create';
import * as React from 'react';

const ImplicitSchemaDefiner = () => {
  const { schema, setSchema } = useOutputState();

  const [isValidSchema, setIsValidSchema] = React.useState(true);

  const onSchemaChange = (val) => {
    try {
      setSchema(JSON.parse(val));
    } catch (e) {
      setIsValidSchema(false);
    }
  };

  return (
    <div>
      <WidgetWrapper
        widgetProperty={{
          field: 'schema',
          name: 'schema',
          'widget-type': 'json-editor',
          'widget-attributes': {
            rows: '20',
          },
        }}
        pluginProperty={{
          required: false,
          name: 'json-editor',
        }}
        value={JSON.stringify(schema, undefined, 2)}
        onChange={onSchemaChange}
      />
    </div>
  );
};

export default ImplicitSchemaDefiner;
