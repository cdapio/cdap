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
import { IComplexTypeNames, ISimpleType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { Nullable } from 'components/AbstractWidget/SchemaEditor/RowButtons/Nullable';
import { IOnchangeHandler } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
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
  type?: ISimpleType | IComplexTypeNames;
}

function RowButtons({
  type,
  nullable,
  onNullable,
  onAdd,
  onRemove,
  onChange,
  typeProperties,
}: IRowButtonsProps) {
  return (
    <RowButtonWrapper>
      <If condition={typeof onNullable === 'function'} invisible>
        <Nullable nullable={nullable} onNullable={onNullable} />
      </If>
      <If condition={typeof onAdd === 'function'} invisible>
        <AddRowButton onAdd={onAdd} />
      </If>
      <If condition={typeof onRemove === 'function'} invisible>
        <RemoveRowButton onRemove={onRemove} />
      </If>
      <If condition={type === 'record' || type === 'enum' || type === 'decimal'}>
        <FieldPropertiesPopoverButton
          nullable={nullable}
          onNullable={onNullable}
          type={type}
          onChange={onChange}
          typeProperties={typeProperties}
        />
      </If>
    </RowButtonWrapper>
  );
}

export { RowButtons };
