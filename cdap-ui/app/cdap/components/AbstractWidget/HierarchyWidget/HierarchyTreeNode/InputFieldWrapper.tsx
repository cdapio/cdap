/*
 * Copyright © 2021 Cask Data, Inc.
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
import If from 'components/If';
import { Box, IconButton, Input, makeStyles, withStyles } from '@material-ui/core';
import KeyboardArrowRightIcon from '@material-ui/icons/KeyboardArrowRight';

const InputFieldContainer = withStyles(() => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '0fr 1fr',
      alignItems: 'center',
      '& [disabled]': {
        cursor: 'pointer',
        background: 'transparent',
      },
    },
  };
})(Box);

const HiddenBtn = withStyles(() => {
  return {
    root: {
      padding: '6.5px',
      visibility: 'hidden',
    },
  };
})(IconButton);

const useStyles = makeStyles(() => {
  return {
    input: {
      marginLeft: '5px',
      padding: '4px 0px 4px 4px',
      border: 0,
      width: '80%',
      '& [disabled]': {
        cursor: 'pointer',
        background: 'transparent',
      },
    },
  };
});

interface INodeProps {
  name: string;
  type: string;
  path?: string[];
  children?: INodeProps[];
}
interface IInputFieldWrapperProps {
  node: INodeProps;
  nameChangeHandler: (value) => void | React.Dispatch<any>;
  arrowIcon?: React.ReactElement;
  disabled?: boolean;
}

const InputFieldWrapper = ({
  node,
  nameChangeHandler,
  arrowIcon,
  disabled,
}: IInputFieldWrapperProps) => {
  const classes = useStyles();
  const showArrow = node.children.length > 0;

  const ArrowIcon = arrowIcon;
  return (
    <InputFieldContainer>
      <If condition={showArrow}>{ArrowIcon}</If>
      <If condition={!showArrow}>
        <HiddenBtn>
          <KeyboardArrowRightIcon />
        </HiddenBtn>
      </If>
      <Input
        type="text"
        onChange={nameChangeHandler}
        className={classes.input}
        value={node.name}
        disabled={typeof node.path !== 'undefined'}
        placeholder="Field Name"
        disableUnderline={true}
        readOnly={disabled}
        data-cy="input"
      />
    </InputFieldContainer>
  );
};

export default InputFieldWrapper;
