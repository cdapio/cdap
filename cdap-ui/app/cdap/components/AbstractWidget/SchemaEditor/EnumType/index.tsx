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
import { IFieldTypeBaseProps } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import { RowButtons } from 'components/AbstractWidget/SchemaEditor/RowButtons';
import TextboxOnValium from 'components/TextboxOnValium';
import makeStyles from '@material-ui/core/styles/makeStyles';

const useStyles = makeStyles({
  textbox: {
    border: 0,
  },
});

const EnumTypeBase = ({
  typeProperties,
  onChange,
  onAdd,
  onRemove,
  autoFocus,
  disabled = false,
}: IFieldTypeBaseProps) => {
  const { symbol } = typeProperties;
  const [enumSymbol, setEnumSymbol] = React.useState(symbol);
  const inputEle = React.useRef(null);
  const classes = useStyles();
  const [fieldTypeProperties, setFieldTypeProperties] = React.useState(typeProperties || {});

  const onTypePropertiesChangeHandler = (property, value) => {
    if (property === 'typeProperties') {
      setFieldTypeProperties(value);
    }
    onChange(property, value);
  };
  const onChangeHandler = (newValue, _, keyPressKeyCode) => {
    if (keyPressKeyCode === 13) {
      onAdd();
      return;
    }
    if (enumSymbol === newValue) {
      return;
    }
    setEnumSymbol(newValue);
    onChange('typeProperties', {
      symbol: newValue,
    });
  };

  React.useEffect(() => {
    if (autoFocus) {
      if (inputEle.current) {
        inputEle.current.focus();
      }
    }
  }, [autoFocus]);
  return (
    <React.Fragment>
      <TextboxOnValium
        disabled={disabled}
        value={enumSymbol}
        onChange={onChangeHandler}
        placeholder="symbol"
        inputRef={(ref) => (inputEle.current = ref)}
        onKeyUp={() => ({})}
        className={classes.textbox}
      />
      <RowButtons
        disabled={disabled}
        onRemove={onRemove}
        onAdd={onAdd}
        typeProperties={fieldTypeProperties}
        onChange={onTypePropertiesChangeHandler}
      />
    </React.Fragment>
  );
};
const EnumType = React.memo(EnumTypeBase);
export { EnumType };
