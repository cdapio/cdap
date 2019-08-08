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
import React from 'react';
import Select from 'components/AbstractWidget/FormInputs/Select';
import { IWidgetProps, IStageSchema } from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';

interface IField {
  name: string;
  type: string;
}

interface IInputFieldProps extends IWidgetProps<null> {}

// We are assuming all incoming stages have the same schema
function getFields(schemas: IStageSchema[]) {
  let fields = [];
  if (!schemas || schemas.length === 0) {
    return fields;
  }
  const stage = schemas[0];

  try {
    const unparsedFields = JSON.parse(stage.schema).fields;

    if (unparsedFields.length > 0) {
      fields = unparsedFields.map((field: IField) => field.name);
    }
  } catch {
    // tslint:disable-next-line: no-console
    console.log('Error: Invalid JSON schema');
  }
  return fields;
}

const InputFieldDropdown: React.FC<IInputFieldProps> = ({
  value,
  onChange,
  disabled,
  extraConfig,
}) => {
  const inputSchema = objectQuery(extraConfig, 'inputSchema');
  const fieldValues = getFields(inputSchema);
  const widgetProps = {
    options: fieldValues,
  };

  return <Select value={value} onChange={onChange} widgetProps={widgetProps} disabled={disabled} />;
};

export default InputFieldDropdown;

(InputFieldDropdown as any).propTypes = WIDGET_PROPTYPES;
