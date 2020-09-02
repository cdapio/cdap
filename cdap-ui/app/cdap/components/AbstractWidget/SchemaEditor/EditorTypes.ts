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

/**
 * Contains types used only by the editor for presentation.
 */
import { ISimpleType, IComplexTypeNames } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { ITypeProperties } from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import {
  OperationTypesEnum,
  InternalTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';

type IInternalFieldType =
  | 'schema'
  | 'record-field-simple-type'
  | 'record-field-complex-type-root'
  | 'array-simple-type'
  | 'array-complex-type'
  | 'array-complex-type-root'
  | 'enum-symbol'
  | 'map-keys-complex-type-root'
  | 'map-keys-simple-type'
  | 'map-values-complex-type-root'
  | 'map-values-simple-type'
  | 'union-simple-type'
  | 'union-complex-type-root';

/**
 * Type of flattened row for rendering.
 * Contains context for that specific row. We have 'ancestors' here to show the
 * indentation for complex types.
 *
 * Every row will have a hidden flag. Potentially every row can be hidden
 * when the user collapses.
 *
 * Rows that has children will have a boolean collapsed.
 */
interface IFlattenRowType {
  id: string;
  name?: string;
  type?: ISimpleType | IComplexTypeNames;
  internalType: InternalTypesEnum;
  nullable?: boolean;
  ancestors: string[];
  typeProperties?: ITypeProperties;
  collapsed?: boolean;
  hidden?: boolean;
}

interface IFieldIdentifier {
  id: string;
  ancestors: string[];
}
type IOnchangeHandler = (property: string, value?: string | boolean | ITypeProperties) => void;
type IRowOnChangeHandler = (id: IFieldIdentifier, payload: IOnChangePayload) => void;
interface IFieldTypeBaseProps {
  name?: string;
  type?: ISimpleType | IComplexTypeNames;
  nullable?: boolean;
  internalType?: InternalTypesEnum;
  typeProperties?: ITypeProperties;
  onChange: IOnchangeHandler;
  onAdd: () => void;
  onRemove: () => void;
  autoFocus?: boolean;
  disabled?: boolean;
}

interface IOnChangePayload {
  property?: string;
  value?: string | boolean | ITypeProperties;
  type:
    | OperationTypesEnum.UPDATE
    | OperationTypesEnum.ADD
    | OperationTypesEnum.REMOVE
    | OperationTypesEnum.COLLAPSE;
}

interface IAttributesComponentProps {
  onChange?: IOnchangeHandler;
  typeProperties: ITypeProperties;
  handleClose: () => void;
  disabled?: boolean;
}

export {
  IInternalFieldType,
  IFlattenRowType,
  IFieldIdentifier,
  IFieldTypeBaseProps,
  IOnChangePayload,
  IAttributesComponentProps,
  IOnchangeHandler,
  IRowOnChangeHandler,
};
