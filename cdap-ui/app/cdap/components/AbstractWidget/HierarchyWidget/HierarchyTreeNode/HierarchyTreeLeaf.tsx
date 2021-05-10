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
import { Checkbox } from '@material-ui/core';
import { makeStyles, Theme } from '@material-ui/core/styles';

interface IHierarchyTreeLeafStyleProps {
  margin: number;
}

const useStyles = makeStyles<Theme, IHierarchyTreeLeafStyleProps>(() => {
  return {
    itemWrapper: {
      width: '100%',
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center',
      marginLeft: (props) => `${props.margin}px`,
    },
  };
});

export interface IChildProps {
  name: string;
  type: string;
  parentIdsArr: string[];
  children: IChildProps[];
}
interface IOptionField {
  name: string;
  type: string;
  parentIdsArr: string[];
  children: IChildProps[];
}
interface IHierarchyTreeLeafProps {
  option: IOptionField;
  selected: boolean;
}

const HierarchyTreeLeaf = ({ option, selected }: IHierarchyTreeLeafProps) => {
  const classes = useStyles({ margin: option.parentIdsArr.length * 6 });

  return (
    <div className={classes.itemWrapper}>
      <div>
        <Checkbox checked={selected} color="primary" />
        {option.name}
        {option.children &&
          option.children.length !== 0 &&
          option.children.map((child: IChildProps) => (
            <HierarchyTreeLeaf
              option={child}
              selected={selected}
              data-cy={`option-${option.name}`}
            />
          ))}
      </div>
      <span>{option.type}</span>
    </div>
  );
};

export default HierarchyTreeLeaf;
