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
import cdapavsc from 'services/cdapavscwrapper';
import {
  SchemaGenerator,
  generateSchemaFromComplexType,
} from 'components/AbstractWidget/SchemaEditor/Context/SchemaGenerator';
import { ISchemaType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { IFlattenRowType } from '../EditorTypes';
import { isNilOrEmpty } from 'services/helpers';
import isEqual from 'lodash/isEqual';

/**
 * Schema validator is independent of the schema editor. It takes in a schema tree
 * and converts to an avro schema and parses the schema through the avsc library
 *
 * The context provides a way to broadcast the error to appropriate row
 *
 * We cannot pin point the error to a specific row as avsc library doesn't
 * provide details on the place of failure. We right now surface the error
 * to the parent to show the error under a tree.
 *
 * This is separate in an effort to push this processing to a web worker in the future.
 * If we are not able write our own avro schema parser we won't be able to pinpoint
 * the error to a specific row. In which we have to recurrsively parse each node
 * in the schema tree to identify specific row of issue. This kind of processing
 * will bring the UI down which is when we will move this to the web worker.
 */
interface ISchemaValidatorProviderBaseState {
  id: string;
  time: number;
  error: string;
}
interface ISchemaValidatorContext {
  validate: (id: string, avroSchema: ISchemaType) => ISchemaValidatorProviderBaseState;
  errorMap: Record<string, ISchemaValidatorProviderBaseState>;
}
interface ISchemaValidatorProviderProps {
  errors?: Record<string, string>;
  reset?: boolean;
}

const SchemaValidatorContext = React.createContext<ISchemaValidatorContext>({
  validate: null,
  errorMap: {},
});
const SchemaValidatorConsumer = SchemaValidatorContext.Consumer;

class SchemaValidatorProvider extends React.Component<ISchemaValidatorProviderProps> {
  public defaultState = {
    errorMap: {},
  };

  public componentWillReceiveProps(nextProps) {
    const { errors, reset } = nextProps;
    if (reset) {
      return this.setState({
        errorMap: {},
      });
    }
    if (!isEqual(errors, this.props.errors) || reset !== this.props.reset) {
      this.setState({
        errorMap: {
          ...this.state.errorMap,
          ...(errors || {}),
        },
      });
    }
  }

  /**
   * The subtree validation takes in a flat field, its ancestors
   * and navigates to the appropriate child in the schema tree and parses
   * the subtree. If it is valid nothing happens. If it is invalid we add the
   * error to the validator context with the id of the immediate parent of the field
   *
   * The error validation consumer is used per row and whichever row is added
   * to the error map gets highlighted with error.
   *
   * The sibliling line connection gets highlighted when parent is higlighted with an
   * error.
   * @param field Current flat row updated by user
   * @param schemaTree schema tree to validate against.
   */
  private isSubTreeValidAvroSchema = (field: IFlattenRowType, schemaTree) => {
    const { ancestors } = field;
    if (!ancestors || (Array.isArray(ancestors) && !ancestors.length)) {
      return;
    }
    if (ancestors.length === 1) {
      try {
        const entireSchema = SchemaGenerator(schemaTree);
        cdapavsc.parse(entireSchema.schema, { wrapUnions: true });
      } catch (e) {
        // TODO: CDAP-17152. We don't have specific fields that are duplicate today.
        // We should improve the way we validate the avro schema
        if (e.message.includes('duplicate field name')) {
          return {
            error: 'There are two or more fields with the same name',
            fieldIdToShowError: ancestors[0],
          };
        }
        return { error: e.message, fieldIdToShowError: ancestors[0] };
      }
      return;
    }
    const validateSchema = (tree) => {
      const avroSchema = generateSchemaFromComplexType(tree.type, tree, tree.nullable);
      try {
        cdapavsc.parse(avroSchema, { wrapUnions: true });
      } catch (e) {
        // TODO: CDAP-17152. We don't have specific fields that are duplicate today.
        // We should improve the way we validate the avro schema
        if (e.message.includes('duplicate field name')) {
          return {
            error: 'There are two or more fields with the same name',
            fieldIdToShowError: tree.id,
          };
        }
        return { error: e.message, fieldIdToShowError: tree.id };
      }
    };
    const goToLowestParent = (parents, tree) => {
      if (parents.length === 1) {
        return validateSchema(tree.children[parents[0]]);
      }
      return goToLowestParent(parents.slice(1), tree.children[parents[0]]);
    };
    return goToLowestParent(ancestors.slice(1), schemaTree);
  };

  private validate = (field, schemaTree) => {
    const errorObj = this.isSubTreeValidAvroSchema(field, schemaTree);
    if (!errorObj) {
      const { errorMap } = this.state;
      if (isNilOrEmpty(errorMap)) {
        return;
      }
      field.ancestors.forEach((ancestorId) => {
        if (errorMap.hasOwnProperty(ancestorId)) {
          delete errorMap[ancestorId];
        }
      });
      if (errorMap.hasOwnProperty(field.id)) {
        delete errorMap[field.id];
      }
      this.setState({ errorMap });
      return;
    }
    const { fieldIdToShowError, error } = errorObj;
    this.setState({
      errorMap: {
        ...this.state.errorMap,
        [fieldIdToShowError]: error,
      },
    });
  };

  public state = {
    ...this.defaultState,
    validate: this.validate.bind(this),
  };

  public render() {
    return (
      <SchemaValidatorContext.Provider value={this.state}>
        {this.props.children}
      </SchemaValidatorContext.Provider>
    );
  }
}
export { SchemaValidatorProvider, SchemaValidatorConsumer };
