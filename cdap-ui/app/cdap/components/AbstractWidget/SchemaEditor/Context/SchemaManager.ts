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

import {
  IFlattenRowType,
  IFieldIdentifier,
  IOnChangePayload,
} from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import uuidV4 from 'uuid/v4';
import isEmpty from 'lodash/isEmpty';
import { INode, parseSchema } from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import { FlatSchema } from 'components/AbstractWidget/SchemaEditor/Context/FlatSchema';
import { ISchemaType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { SchemaGenerator } from 'components/AbstractWidget/SchemaEditor/Context/SchemaGenerator';
import isObject from 'lodash/isObject';
import {
  branchCount,
  initChildren,
  getInternalType,
  initTypeProperties,
} from 'components/AbstractWidget/SchemaEditor/Context/SchemaManagerUtilities';
import { isNilOrEmpty } from 'services/helpers';
import {
  InternalTypesEnum,
  OperationTypesEnum,
  getDefaultEmptyAvroSchema,
  AvroSchemaTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';

interface ISchemaManagerOptions {
  collapseAll: boolean;
}

/**
 * Interface that defines the schema data structure.
 */
interface ISchemaManager {
  getSchemaTree: () => INode;
  getFlatSchema: () => IFlattenRowType[];
  getAvroSchema: () => ISchemaType;
  onChange: (fieldId: IFieldIdentifier, onChangePayload: IOnChangePayload) => IOnChangeReturnType;
}

interface IOnChangeReturnType {
  fieldIdToFocus?: string;
  fieldIndex?: number;
  nodeDepth?: number;
}

/**
 * The Schema Manager is the central place to manage the entire schema
 *
 * Internally it has two data-structures,
 * 1. A tree to convert the JSON avro schema to a representation that we can
 * process a little bit faster.
 * 2. A flat array for presentation purpose.
 *
 * The tree is used for adding/removing/deleting fields in an avro schema.
 * It is a basic tree with id, name, type and children.
 *
 * The children is usually a map of id to the child node. In specific cases (records and enums)
 * we need to maintain the order. So in those cases the children map will have a static 'order'
 * array which maintains the order of the children.
 *
 * The flatten array is derived from the tree. It is nothing but a DFS traversal of the tree
 *
 * We need two representations because we need one for processing and the other for display purposes.
 * Flattening the tree as aray helps us with using virtual scroll which improves performance.
 */
class SchemaManagerBase implements ISchemaManager {
  private schemaTree: INode;
  private flatTree: IFlattenRowType[];
  private options: ISchemaManagerOptions;
  constructor(avroSchema, options: ISchemaManagerOptions) {
    this.schemaTree = parseSchema(avroSchema);
    this.flatTree = FlatSchema(this.schemaTree, options);
    this.options = options;
  }

  public getSchemaTree = () => this.schemaTree;
  public getFlatSchema = () => this.flatTree;
  public getAvroSchema = () => SchemaGenerator(this.schemaTree);

  // Generic function to insert the newly created child id into the order array
  // to maintain order of fields.
  private insertNewIdToOrder = (order = [], referenceId) => {
    const id = uuidV4();
    // +1 to add next to the current element.
    const currentIndexOfChild = order.findIndex((c) => c === referenceId) + 1;
    order = [...order.slice(0, currentIndexOfChild), id, ...order.slice(currentIndexOfChild)];
    return { id, order };
  };

  private addNewEnumSymbol = (tree: INode, fieldId: IFieldIdentifier) => {
    if (!tree.children || (tree.children && !Array.isArray(tree.children.order))) {
      return { tree, newTree: undefined, currentField: undefined };
    }
    const { id = uuidV4(), order = [] } = this.insertNewIdToOrder(
      tree.children.order as string[],
      fieldId.id
    );
    tree.children.order = order;
    const newlyAddedField = {
      id,
      internalType: InternalTypesEnum.ENUM_SYMBOL,
      typeProperties: {
        symbol: '',
      },
    };
    tree.children[id] = newlyAddedField;
    return {
      tree,
      newTree: tree.children[id],
      currentField: tree.children[fieldId.id],
      newlyAddedField,
    };
  };

  private addNewFieldType = (tree: INode, fieldId: IFieldIdentifier) => {
    if (!tree.children || (tree.children && !Array.isArray(tree.children.order))) {
      return { tree, newTree: undefined, currentField: undefined };
    }
    const { id = uuidV4(), order = [] } = this.insertNewIdToOrder(
      tree.children.order as string[],
      fieldId.id
    );
    tree.children.order = order;
    const newlyAddedField = {
      id,
      internalType: InternalTypesEnum.RECORD_SIMPLE_TYPE,
      nullable: false,
      type: AvroSchemaTypesEnum.STRING,
      name: '',
    };
    tree.children[id] = newlyAddedField;
    return {
      tree,
      newTree: tree.children[id],
      currentField: tree.children[fieldId.id],
      newlyAddedField,
    };
  };

  private addNewUnionType = (tree: INode, fieldId: IFieldIdentifier) => {
    if (!tree.children || (tree.children && !Array.isArray(tree.children.order))) {
      return { tree, newTree: undefined, currentField: undefined };
    }
    const { id = uuidV4(), order = [] } = this.insertNewIdToOrder(
      tree.children.order as string[],
      fieldId.id
    );
    tree.children.order = order;
    const newlyAddedField = {
      id,
      internalType: InternalTypesEnum.UNION_SIMPLE_TYPE,
      nullable: false,
      type: AvroSchemaTypesEnum.STRING,
    };
    tree.children[id] = newlyAddedField;
    return {
      tree,
      newTree: tree.children[id],
      currentField: tree.children[fieldId.id],
      newlyAddedField,
    };
  };

  private addSpecificTypesToTree = (tree: INode, fieldId: IFieldIdentifier) => {
    switch (tree.type) {
      case AvroSchemaTypesEnum.ENUM:
        return this.addNewEnumSymbol(tree, fieldId);
      case AvroSchemaTypesEnum.RECORD:
        return this.addNewFieldType(tree, fieldId);
      case AvroSchemaTypesEnum.UNION:
        return this.addNewUnionType(tree, fieldId);
      default:
        return { tree: undefined, newTree: undefined, currentField: undefined };
    }
  };

  private addToTree = (
    tree: INode,
    fieldId: IFieldIdentifier
  ): {
    tree: INode;
    newTree: INode;
    currentField: INode;
  } => {
    if (!tree) {
      return { tree: undefined, newTree: undefined, currentField: undefined };
    }
    // Only the top level record fields will have one ancestors. So just add the specific type.
    if (fieldId.ancestors.length === 1) {
      return this.addSpecificTypesToTree(tree, fieldId);
    }
    // Traverse to the specific child tree in the main schema tree.
    const { tree: child, newTree, currentField } = this.addToTree(
      tree.children[fieldId.ancestors[1]],
      { id: fieldId.id, ancestors: fieldId.ancestors.slice(1) }
    );
    // Mutates the main schema tree. This a performance optimization that we need to
    // to avoid generating huge JSONs on every mutation of the schema.
    return {
      tree: {
        ...tree,
        children: {
          ...tree.children,
          [child.id]: child,
        },
      },
      newTree,
      currentField,
    };
  };

  /**
   * Add a new field to the current index. This could mean,
   *  - Add a new field in record schema
   *  - Add a new Symbol to enum
   *  - Add a new union type
   * The results of addition are,
   *  - Modifying the schema tree to add the new field (tree)
   *  - Newly added tree
   *  - Current field to focus on
   * We don't re-flatten the entire schema tree all the time. We calculate only the newly
   * added field, it could be a single field or a tree in case of union or map or enum,
   * and then flatten that sub tree and insert into our flattened array.
   *
   * @param fieldId ID of the current row being updated.
   */
  private add = (fieldId: IFieldIdentifier): IOnChangeReturnType => {
    const currentIndex = this.flatTree.findIndex((f) => f.id === fieldId.id);
    const matchingEntry = this.flatTree[currentIndex];
    let result: { tree: INode; newTree: INode; currentField: INode };
    let newFlatSubTree: IFlattenRowType[];
    const idObj = { id: matchingEntry.id, ancestors: matchingEntry.ancestors };
    // The result is the newly modified tree, the newly added tree and the current field
    // The modified tree is the entire schema tree that we maintain
    // The new tree is for flattening and inserting it into the flattened array
    // The current field is then passed on to the schema editor for presentation (focus on that specific row)
    result = this.addToTree(this.schemaTree, idObj);
    const customOptions = {
      ...this.options,
      collapseAll: false,
    };
    newFlatSubTree = FlatSchema(result.newTree, customOptions, matchingEntry.ancestors);
    this.schemaTree = result.tree;
    const currentFieldBranchCount = branchCount(result.currentField);
    this.flatTree = [
      ...this.flatTree.slice(0, currentIndex + currentFieldBranchCount + 1),
      ...newFlatSubTree,
      ...this.flatTree.slice(currentIndex + currentFieldBranchCount + 1),
    ];
    return {
      fieldIdToFocus: this.flatTree[currentIndex + currentFieldBranchCount + 1].id,
      fieldIndex: currentIndex + currentFieldBranchCount + 1,
    };
  };

  private removeFromTree = (tree: INode, fieldId) => {
    if (!tree) {
      return { tree: undefined };
    }
    if (fieldId.ancestors.length === 1) {
      const field = { ...tree.children[fieldId.id] };
      let newField;
      if (Array.isArray(tree.children.order) && Object.keys(tree.children).length === 2) {
        const {
          tree: treeWithDefaultChild,
          newlyAddedField: defaultNewField,
        } = this.addSpecificTypesToTree(tree, fieldId);
        newField = defaultNewField;
        tree = treeWithDefaultChild;
      }
      if (Array.isArray(tree.children.order)) {
        tree.children.order = tree.children.order.filter((id) => id !== fieldId.id);
      }
      delete tree.children[fieldId.id];
      return { tree, removedField: field, newlyAddedField: newField };
    }
    const { tree: newTree, removedField, newlyAddedField } = this.removeFromTree(
      tree.children[fieldId.ancestors[1]],
      {
        id: fieldId.id,
        ancestors: fieldId.ancestors.slice(1),
      }
    );
    return {
      tree: {
        ...tree,
        children: {
          ...tree.children,
          ...newTree,
        },
      },
      removedField,
      newlyAddedField,
    };
  };

  /**
   * Remove a field from the schema. This could be a simple type of row
   * or a complex type.
   * There is also a need for us to add a new row as part of removal.
   *
   * For instance: This comes up when users delete all the fields in
   * a record type. We can't have all the fields deleted. Upon the
   * deleting the final field we still need to add a new field for
   * the user to be able to keep the record type.
   * @param fieldId ID of the current row being updated.
   */
  private remove = (fieldId: IFieldIdentifier): IOnChangeReturnType => {
    const currentIndex = this.flatTree.findIndex((f) => f.id === fieldId.id);
    const matchingEntry = this.flatTree[currentIndex];
    const idObj = { id: matchingEntry.id, ancestors: matchingEntry.ancestors };
    const { tree, removedField, newlyAddedField } = this.removeFromTree(this.schemaTree, idObj);
    this.schemaTree = tree;
    // branch count to determine the slice in the flattened array.
    // If the user removed a complex type we need to remove the row
    // and all of its children.
    const childrenInBranch = branchCount(removedField);
    let newFlatSubTree = [];
    // Newly added row in case we need to maintain the structure of say, a record or an enum.
    if (newlyAddedField) {
      const customOptions = {
        ...this.options,
        collapseAll: false,
      };
      newFlatSubTree = FlatSchema(newlyAddedField, customOptions, matchingEntry.ancestors);
    }
    this.flatTree = [
      ...this.flatTree.slice(0, currentIndex),
      ...newFlatSubTree,
      ...this.flatTree.slice(currentIndex + 1 + childrenInBranch), // remove current row along with its children.
    ];
    return {
      fieldIdToFocus: this.flatTree[currentIndex - 1].id,
      fieldIndex: currentIndex - 1,
    };
  };

  private updateTree = (
    tree: INode,
    fieldId: IFieldIdentifier,
    { property, value }: Partial<IOnChangePayload>
  ): {
    tree: INode;
    childrenCount: number;
    newTree: INode;
  } => {
    if (!tree) {
      return { childrenCount: undefined, tree: undefined, newTree: undefined };
    }
    if (fieldId.ancestors.length === 1 && !isEmpty(tree.children[fieldId.id])) {
      if (property === 'typeProperties' && typeof value === 'object') {
        tree.children[fieldId.id][property] = {
          ...tree.children[fieldId.id][property],
          ...value,
        };
      } else {
        tree.children[fieldId.id][property] = value;
      }
      let childrenInBranch = 0;
      let newChildTree: INode;
      if (property === 'type') {
        childrenInBranch = branchCount(tree.children[fieldId.id]);
        tree.children[fieldId.id].children = initChildren(value);
        newChildTree = tree.children[fieldId.id];
        tree.children[fieldId.id].internalType = getInternalType(tree.children[fieldId.id]);
        tree.children[fieldId.id].typeProperties = initTypeProperties(tree.children[fieldId.id]);
      }
      return {
        tree,
        childrenCount: childrenInBranch,
        newTree: newChildTree,
      };
    }

    const { tree: child, childrenCount, newTree } = this.updateTree(
      tree.children[fieldId.ancestors[1]],
      { id: fieldId.id, ancestors: fieldId.ancestors.slice(1) },
      { property, value }
    );
    return {
      tree: {
        ...tree,
        children: {
          ...tree.children,
          [child.id]: child,
        },
      },
      childrenCount,
      newTree,
    };
  };

  /**
   * Updating a field could be changing the 'name' or the 'type'.
   *
   * Updating 'name' is pretty straight forward and involves no complicated steps.
   * Updating 'type' involves changing from a simple type to a complex type or vice-versa.
   *
   * This again involves addition or removal of nodes in the schema.
   * @param fieldId ID of the current row being updated.
   * @param onChangePayload - { property, value } - payload for updating.
   */
  private update = (
    fieldId: IFieldIdentifier,
    { property, value }: Partial<IOnChangePayload>
  ): IOnChangeReturnType => {
    const index = this.flatTree.findIndex((f) => f.id === fieldId.id);
    if (property === 'typeProperties' && typeof value === 'object') {
      this.flatTree[index][property] = {
        ...this.flatTree[index][property],
        ...value,
      };
    } else {
      this.flatTree[index][property] = value;
    }
    const matchingEntry = this.flatTree[index];
    let result: { tree: INode; childrenCount: number; newTree: INode };
    let newFlatSubTree: IFlattenRowType[];
    const idObj = { id: matchingEntry.id, ancestors: matchingEntry.ancestors };
    result = this.updateTree(this.schemaTree, idObj, { property, value });
    this.schemaTree = result.tree;
    // If user changed a complex type to a simple type or to another complex type we need
    // to remove the complex type children first
    this.flatTree = [
      ...this.flatTree.slice(0, index),
      ...this.flatTree.slice(index + result.childrenCount + (!result.newTree ? 0 : 1)),
    ];
    const customOptions = {
      ...this.options,
      collapseAll: false,
    };
    // And then add the newly updated complex type children to the flattened array.
    if (result.newTree) {
      newFlatSubTree = FlatSchema(result.newTree, customOptions, matchingEntry.ancestors);
      this.flatTree = [
        ...this.flatTree.slice(0, index),
        ...newFlatSubTree,
        ...this.flatTree.slice(index),
      ];
    }
    // newFlatSubTree will be of length 1 for simple type changes.
    if (Array.isArray(newFlatSubTree) && newFlatSubTree.length > 1) {
      return {
        fieldIdToFocus: this.flatTree[index + 1].id,
        fieldIndex: index + 1,
      };
    }
    return {};
  };

  /**
   * This is to identify the root tree from the flattened object.
   * @param fieldObj - id of the flattened row
   * @param schemaTree - Tree to find the root node.
   */
  private getFieldObjFromTree = (fieldObj, schemaTree: INode): INode => {
    if (fieldObj.id === schemaTree.id) {
      return schemaTree;
    }
    if (fieldObj.ancestors.length === 1) {
      return schemaTree.children[fieldObj.id];
    }
    return this.getFieldObjFromTree(
      { id: fieldObj.id, ancestors: fieldObj.ancestors.slice(1) },
      schemaTree.children[fieldObj.ancestors[1]]
    );
  };

  /**
   * Function to collapse/expand a tree.
   * @param fieldId - id of the field for which the children needs to be
   * collapsed or expanded.
   */
  private collapse = (fieldId: IFieldIdentifier): IOnChangeReturnType => {
    const matchingIndex = this.flatTree.findIndex((row) => row.id === fieldId.id);
    const matchingEntry = this.flatTree[matchingIndex];
    if (!matchingEntry) {
      return {};
    }
    const idObj = { id: matchingEntry.id, ancestors: matchingEntry.ancestors };
    const fieldObj = this.getFieldObjFromTree(idObj, this.getSchemaTree());
    this.flatTree[matchingIndex].collapsed = !this.flatTree[matchingIndex].collapsed;
    const nodeDepth = this.calculateNodeDepthMap(fieldObj);
    for (let i = 1; i <= nodeDepth; i++) {
      const currentRow = this.flatTree[matchingIndex + i];
      const { collapsed } = currentRow;
      if (typeof collapsed === 'boolean') {
        if (collapsed === true) {
          const childTreeDepth = this.calculateNodeDepthMap(
            this.getFieldObjFromTree(currentRow, this.getSchemaTree())
          );
          this.flatTree[matchingIndex + i].hidden = this.flatTree[matchingIndex].collapsed;
          i += childTreeDepth;
          continue;
        } else {
          this.flatTree[matchingIndex + i].collapsed = this.flatTree[matchingIndex].collapsed;
        }
      }
      this.flatTree[matchingIndex + i].hidden = this.flatTree[matchingIndex].collapsed;
    }
    return {};
  };

  /**
   * Identify the depth of a parent node. Depth here defines all the immediate children and all
   * the children of childrens and so on and so forth. We need to calculate this depth because
   * we do a DFS of our tree to flatten. Now if we need to collapse a subtree we need all the children
   * (both direct and indirect) because in a flattened array the children comes first before the sibling.
   * @param tree - Root node to find the depth.
   */
  private calculateNodeDepthMap = (tree: INode): number => {
    let totalDepth = 0;
    if (isObject(tree.children) && Object.keys(tree.children).length) {
      totalDepth += Object.keys(tree.children).filter((c) => c !== 'order').length;
      for (const childId of Object.keys(tree.children)) {
        if (childId === 'order') {
          continue;
        }
        const childCount = this.calculateNodeDepthMap(tree.children[childId]);
        totalDepth += childCount;
      }
    }
    return totalDepth;
  };

  /**
   * The generic onChange is supposed to handle all types of mutation to the schema.
   * This onChange is the handler for any changes that happen in the schema (not to be confused with
   * the onChange in the schemaEditor).
   * @param fieldId - unique id to identify every field in the schema tree.
   * @param onChangePayload {type, property, value} - Every onchange call will have the type of
   * change, the property and the value. The type could be 'add', 'update', 'remove' and property could be
   * 'name' or 'typeProperties'
   */
  public onChange = (
    fieldId: IFieldIdentifier,
    { type, property, value }: IOnChangePayload
  ): IOnChangeReturnType => {
    if (isNilOrEmpty(fieldId)) {
      return;
    }
    switch (type) {
      case OperationTypesEnum.UPDATE:
        return this.update(fieldId, { property, value });
      case OperationTypesEnum.ADD:
        return this.add(fieldId);
      case OperationTypesEnum.REMOVE:
        return this.remove(fieldId);
      case OperationTypesEnum.COLLAPSE:
        return this.collapse(fieldId);
    }
  };
}

/**
 * Default options for the schema manager. This as of now only has collapseAll.
 * Will expand to restricting schema types.
 */
const defaultOptions: ISchemaManagerOptions = {
  collapseAll: true,
};

function SchemaManager(
  avroSchema = getDefaultEmptyAvroSchema(),
  options: ISchemaManagerOptions = defaultOptions
) {
  if (!options) {
    options = defaultOptions;
  } else {
    options = {
      ...defaultOptions,
      ...options,
    };
  }
  const schemaTreeInstance = new SchemaManagerBase(avroSchema, options);
  return {
    getInstance: () => schemaTreeInstance,
  };
}
export { SchemaManager, ISchemaManager, IOnChangeReturnType, ISchemaManagerOptions };
