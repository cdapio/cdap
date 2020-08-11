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
import withStyles from '@material-ui/core/styles/withStyles';
import Box from '@material-ui/core/Box';
import { AddRowButton } from 'components/AbstractWidget/SchemaEditor/RowButtons/AddRowButton';
import { RemoveRowButton } from 'components/AbstractWidget/SchemaEditor/RowButtons/RemoveRowButton';
import { FieldPropertiesPopoverButton } from 'components/AbstractWidget/SchemaEditor/RowButtons/FieldAttributes/FieldAttributesPopoverButton';
import If from 'components/If';
import { Nullable } from 'components/AbstractWidget/SchemaEditor/RowButtons/Nullable';
import { IOnchangeHandler } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import {
  AvroSchemaTypesEnum,
  InternalTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
/**
 * Generic row buttons (add, nullable & remove buttons)
 * Based on the availability of handlers for each action each
 * button is showed.
 */
const RowButtonWrapper = withStyles(() => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '24px 24px 24px 24px',
      gridTemplateRows: '24px',
      '& [disabled]': {
        cursor: 'not-allowed',
      },
    },
  };
})(Box);

interface IRowButtonsProps {
  nullable?: boolean;
  onNullable?: (checked: boolean) => void;
  onAdd?: () => void;
  onRemove?: () => void;
  onChange?: IOnchangeHandler;
  typeProperties?: Record<string, string>;
  type?: AvroSchemaTypesEnum;
  disabled?: boolean;
  internalType?: InternalTypesEnum;
}

function RowButtons({
  type,
  nullable,
  onNullable,
  onAdd,
  onRemove,
  onChange,
  typeProperties,
  disabled = false,
  internalType,
}: IRowButtonsProps) {
  return (
    <RowButtonWrapper disabled={disabled}>
      <If condition={typeof onNullable === 'function'} invisible>
        <Nullable nullable={nullable} onNullable={disabled ? undefined : onNullable} />
      </If>
      <If condition={typeof onAdd === 'function'} invisible>
        <AddRowButton onAdd={disabled ? undefined : onAdd} />
      </If>
      <If condition={typeof onRemove === 'function'} invisible>
        <RemoveRowButton onRemove={disabled ? undefined : onRemove} />
      </If>
      <If
        condition={
          type === AvroSchemaTypesEnum.RECORD ||
          type === AvroSchemaTypesEnum.ENUM ||
          type === AvroSchemaTypesEnum.DECIMAL ||
          internalType === InternalTypesEnum.RECORD_COMPLEX_TYPE_ROOT ||
          internalType === InternalTypesEnum.RECORD_SIMPLE_TYPE
        }
      >
        <FieldPropertiesPopoverButton
          nullable={nullable}
          onNullable={disabled ? undefined : onNullable}
          type={type}
          onChange={disabled ? undefined : onChange}
          typeProperties={typeProperties}
          disabled={disabled}
          internalType={internalType}
        />
      </If>
    </RowButtonWrapper>
  );
}

export { RowButtons };
