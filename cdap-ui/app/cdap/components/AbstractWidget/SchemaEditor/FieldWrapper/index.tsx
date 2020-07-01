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
import Box from '@material-ui/core/Box';
import Paper from '@material-ui/core/Paper';
import isObject from 'lodash/isObject';
import withStyles from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import { SiblingCommunicationConsumer } from 'components/AbstractWidget/SchemaEditor/FieldWrapper/SiblingCommunicationContext';
import { SchemaValidatorConsumer } from '../SchemaValidator';
import isNil from 'lodash/isNil';
import { SiblingLine } from 'components/AbstractWidget/SchemaEditor/FieldWrapper/SiblingLine';
import {
  INDENTATION_SPACING,
  rowHeight,
  rowMarginTop,
} from 'components/AbstractWidget/SchemaEditor/FieldWrapper/FieldWrapperConstants';

interface IFieldWrapperProps {
  ancestors: string[];
  children?: React.ReactNode;
  style?: any;
  className?: any;
}

const CustomizedPaper = withStyles(() => {
  return {
    root: {
      padding: '2px 10px 2px 0px',
      display: 'grid',
      marginTop: `${rowMarginTop}px`,
      gridTemplateRows: `${rowHeight}px`,
      position: 'relative',
    },
  };
})(Paper);

const SiblingsWrapper = withStyles(() => {
  return {
    root: {
      position: 'absolute',
      left: 0,
      top: 0,
    },
  };
})(Box);

const FieldWrapperBase = ({
  ancestors = [],
  children,
  style = {},
  className,
}: IFieldWrapperProps) => {
  /**
   * Based on the number of ancestors we indent the row accordingly. Each ancestor will
   * have a line indicating hierarchy and a horizontal line with immediate parent
   *
   * The design is to have a grid base row
   * - There is a single column wrapper (for unions and arrays) which is implemented by SingleColumnWrapper
   * - There is a two column layout for field name/type (or label and type for maps) and the
   * other for the row buttons. This helps us maintain the vertical alignment for row buttons
   * no matter what the indentation is.
   *
   * The width of the wrapper is reduced based on the indentation.
   */
  const spacing = ancestors.length * INDENTATION_SPACING;
  const firstColumn = '20px';
  const thirdColumn = `96px`;
  const errorColumn = '10px';
  const secondColumn = `calc(100% - (${firstColumn} + ${thirdColumn} + ${errorColumn}))`;
  let customStyles: Partial<CSSStyleDeclaration> = {
    marginLeft: `${spacing}px`,
    gridTemplateColumns: `${firstColumn} ${secondColumn} ${thirdColumn}`,
    width: `calc(100% - ${spacing + 5 /* box shadow */}px)`,
    alignItems: 'center',
  };
  if (style && isObject(style)) {
    customStyles = {
      ...customStyles,
      ...style,
    };
  }
  return (
    <CustomizedPaper elevation={2} style={customStyles} className={className}>
      <If condition={ancestors.length > 1}>
        <SchemaValidatorConsumer>
          {({ errorMap }) => {
            return (
              <SiblingCommunicationConsumer>
                {({ activeParent, setActiveParent }) => {
                  return (
                    <SiblingsWrapper>
                      {ancestors.slice(1).map((id, index) => {
                        return (
                          <SiblingLine
                            key={id}
                            id={id}
                            activeParent={activeParent}
                            setActiveParent={setActiveParent}
                            ancestors={ancestors}
                            index={index}
                            error={!isNil(errorMap[id])}
                          />
                        );
                      })}
                    </SiblingsWrapper>
                  );
                }}
              </SiblingCommunicationConsumer>
            );
          }}
        </SchemaValidatorConsumer>
      </If>
      {children}
    </CustomizedPaper>
  );
};

const FieldInputWrapperBase = withStyles(() => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: 'auto 100px',
    },
  };
})(Box);

const FieldWrapper = React.memo(FieldWrapperBase);
const FieldInputWrapper = React.memo(FieldInputWrapperBase);
export { FieldWrapper, FieldInputWrapper, rowHeight, rowMarginTop };
