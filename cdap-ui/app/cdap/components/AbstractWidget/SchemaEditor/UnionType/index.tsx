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
import { schemaTypes } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import { SingleColumnWrapper } from 'components/AbstractWidget/SchemaEditor/SingleColumnWrapper';
import Select from 'components/AbstractWidget/FormInputs/Select';
import { IFieldTypeBaseProps } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import { RowButtons } from 'components/AbstractWidget/SchemaEditor/RowButtons';

const UnionTypeBase = ({
  type,
  onChange,
  onAdd,
  onRemove,
  autoFocus,
  typeProperties,
  disabled = false,
}: IFieldTypeBaseProps) => {
  const [fieldType, setFieldType] = React.useState(type);
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
  return (
    <React.Fragment>
      <SingleColumnWrapper>
        <Select
          disabled={disabled}
          value={fieldType}
          onChange={(newValue) => {
            setFieldType(newValue);
            onChange('type', newValue);
          }}
          widgetProps={{ options: schemaTypes, dense: true, native: true }}
          inputRef={(ref) => (inputEle.current = ref)}
        />
      </SingleColumnWrapper>
      <RowButtons
        disabled={disabled}
        onAdd={onAdd}
        onRemove={onRemove}
        type={fieldType}
        onChange={onTypePropertiesChangeHandler}
        typeProperties={fieldTypeProperties}
      />
    </React.Fragment>
  );
};
const UnionType = React.memo(UnionTypeBase);
export { UnionType };
