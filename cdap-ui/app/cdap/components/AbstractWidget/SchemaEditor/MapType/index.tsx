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
import Select from 'components/AbstractWidget/FormInputs/Select';
import {
  schemaTypes,
  InternalTypesEnum,
  AvroSchemaTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import Box from '@material-ui/core/Box';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import { IFieldTypeBaseProps } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import { RowButtons } from 'components/AbstractWidget/SchemaEditor/RowButtons';

const MapWrapper = withStyles(
  (): StyleRules => {
    return {
      root: {
        display: 'grid',
        gridTemplateColumns: '35px 100px',
        alignItems: 'center',
        gridGap: '10px',
      },
    };
  }
)(Box);

const MapTypeBase = ({
  internalType,
  type,
  nullable,
  onChange,
  autoFocus,
  typeProperties,
  disabled = false,
}: IFieldTypeBaseProps) => {
  let label = '';
  const keysType: string[] = [
    InternalTypesEnum.MAP_KEYS_COMPLEX_TYPE_ROOT,
    InternalTypesEnum.MAP_KEYS_SIMPLE_TYPE,
  ];
  const valuesType: string[] = [
    InternalTypesEnum.MAP_VALUES_COMPLEX_TYPE_ROOT,
    InternalTypesEnum.MAP_VALUES_SIMPLE_TYPE,
  ];
  if (keysType.indexOf(internalType) !== -1) {
    label = 'Keys: ';
  }
  if (valuesType.indexOf(internalType) !== -1) {
    label = 'Values: ';
  }
  const [fieldType, setFieldType] = React.useState(type);
  const [fieldNullable, setFieldNullable] = React.useState(nullable);
  const [fieldTypeProperties, setFieldTypeProperties] = React.useState(typeProperties || {});

  const onTypePropertiesChangeHandler = (property, value) => {
    if (property === 'typeProperties') {
      setFieldTypeProperties(value);
    }
    onChange(property, value);
  };
  const inputEle = React.useRef(null);
  React.useEffect(() => {
    if (autoFocus) {
      if (inputEle.current) {
        inputEle.current.focus();
      }
    }
  }, [autoFocus]);
  const onNullable = (checked) => {
    setFieldNullable(checked);
    onChange('nullable', checked);
  };
  return (
    <React.Fragment>
      <MapWrapper>
        <span>{label}</span>
        <Select
          disabled={disabled}
          value={fieldType}
          onChange={(newValue) => {
            setFieldType(newValue);
            onChange('type', newValue);
          }}
          widgetProps={{ options: schemaTypes, dense: true, inline: true, native: true }}
          inputRef={(ref) => (inputEle.current = ref)}
        />
      </MapWrapper>
      <RowButtons
        disabled={disabled}
        nullable={fieldNullable}
        onNullable={type === AvroSchemaTypesEnum.UNION ? undefined : onNullable}
        type={fieldType}
        onChange={onTypePropertiesChangeHandler}
        typeProperties={fieldTypeProperties}
      />
    </React.Fragment>
  );
};
const MapType = React.memo(MapTypeBase);
export { MapType };
