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
import { FieldInputWrapper } from 'components/AbstractWidget/SchemaEditor/FieldWrapper';
import Select from 'components/AbstractWidget/FormInputs/Select';
import {
  schemaTypes,
  AvroSchemaTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import { IFieldTypeBaseProps } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import { RowButtons } from 'components/AbstractWidget/SchemaEditor/RowButtons';
import TextboxOnValium from 'components/TextboxOnValium';
import makeStyles from '@material-ui/core/styles/makeStyles';
import { ISimpleType, IComplexTypeNames } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';

const useStyles = makeStyles({
  textbox: {
    border: 0,
  },
});

const FieldTypeBase = ({
  name,
  type,
  nullable,
  onChange,
  onAdd,
  onRemove,
  autoFocus,
  typeProperties,
  disabled = false,
  internalType,
}: IFieldTypeBaseProps) => {
  /**
   * We use hooks here because we propagte the state only upwards
   * via onChange. Any intermediate state is stored here.
   *
   * For instance when the user sets the precision and scale of a decimal type we
   * set that in typeProperties here as part of the hooks for immediate feedback
   * but at the same time propagate the value via onChange to the parent.
   *
   * This state is stored as long as the component is not destroyed. on Unmount and
   * remount (happens as part of the scroll) we get the values from the parent once
   * on mount. So the values will be the same as it was last propagated.
   *
   * This holds the same for all field type (array, enum, map & union).
   */
  const [fieldName, setFieldName] = React.useState(name);
  const [fieldType, setFieldType] = React.useState<ISimpleType | IComplexTypeNames>(type);
  const [fieldNullable, setFieldNullable] = React.useState(nullable);
  const [fieldTypeProperties, setFieldTypeProperties] = React.useState(typeProperties || {});
  const inputEle = React.useRef(null);
  const classes = useStyles();
  React.useEffect(() => {
    if (autoFocus) {
      if (inputEle.current) {
        inputEle.current.focus();
        inputEle.current.select();
      }
    }
  }, [autoFocus]);
  const onNullable = (checked) => {
    setFieldNullable(checked);
    onChange('nullable', checked);
  };
  const onChangeHandler = (newValue, _, keyPressKeyCode) => {
    if (newValue !== fieldName) {
      setFieldName(newValue);
      onChange('name', newValue);
    }
    if (keyPressKeyCode === 13) {
      onAdd();
    }
  };

  const onTypeChangeHandler = (newValue) => {
    setFieldType(newValue);
    onChange('type', newValue);
  };
  const inputRef = (ref) => {
    inputEle.current = ref;
  };
  const onTypePropertiesChangeHandler = (property, value) => {
    if (property === 'typeProperties') {
      setFieldTypeProperties(value);
    }
    onChange(property, value);
  };

  return (
    <React.Fragment>
      <FieldInputWrapper>
        <TextboxOnValium
          className={classes.textbox}
          value={fieldName}
          onChange={onChangeHandler}
          placeholder="Field name"
          inputRef={inputRef}
          onKeyUp={() => ({})}
        />
        <Select
          disabled={disabled}
          value={fieldType}
          onChange={onTypeChangeHandler}
          widgetProps={{
            options: schemaTypes,
            dense: true,
            fullWidth: false,
            inputProps: { title: fieldType },
            native: true,
          }}
        />
      </FieldInputWrapper>
      <RowButtons
        disabled={disabled}
        nullable={fieldNullable}
        onNullable={type === AvroSchemaTypesEnum.UNION ? undefined : onNullable}
        type={fieldType}
        onAdd={onAdd}
        onRemove={onRemove}
        onChange={onTypePropertiesChangeHandler}
        typeProperties={fieldTypeProperties}
        internalType={internalType}
      />
    </React.Fragment>
  );
};

const FieldType = React.memo(FieldTypeBase);
export { FieldType };
