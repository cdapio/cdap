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

import isObject from 'lodash/isObject';
import { INode } from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import { IFlattenRowType } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import { ISchemaManagerOptions } from 'components/AbstractWidget/SchemaEditor/Context/SchemaManager';
import { InternalTypesEnum } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';

/**
 * DFS traversal of the schema tree to flatten.
 */
function FlatSchemaBase(
  schemaTree: INode,
  options: ISchemaManagerOptions,
  ancestors = [],
  isParentCollapsed = false
) {
  const result: IFlattenRowType[] = [];
  if (!schemaTree) {
    return [];
  }
  const { internalType, name, id, children, type, typeProperties, nullable } = schemaTree;
  const hasChildren = isObject(children) && Object.keys(children).length;
  const collapsed =
    hasChildren && internalType !== InternalTypesEnum.SCHEMA ? options.collapseAll : null;
  result.push({
    internalType,
    name,
    id,
    type,
    typeProperties,
    ancestors,
    nullable,
    collapsed,
    hidden: isParentCollapsed,
  });
  if (hasChildren) {
    let iterable;
    if (Array.isArray(children.order) && children.order.length) {
      iterable = children.order;
      for (const childId of iterable) {
        result.push(...FlatSchemaBase(children[childId], options, ancestors.concat(id), collapsed));
      }
    } else {
      iterable = children;
      for (const [_, value] of Object.entries<INode>(iterable)) {
        result.push(...FlatSchemaBase(value, options, ancestors.concat(id), collapsed));
      }
    }
  }
  return result;
}

/**
 * Flatten any schema tree node (node + children) to an array.
 * This is generic to be used on the main avro schema tree as well as any sub tree.
 * @param schemaTree Avro schema tree to flatten
 * @param options Options to flatten.
 * @param ancestors Ancestors of the current tree. The main schema tree will have no ancestors.
 */
function FlatSchema(schemaTree: INode, options: ISchemaManagerOptions, ancestors = []) {
  return FlatSchemaBase(schemaTree, options, ancestors);
}
export { FlatSchema };
