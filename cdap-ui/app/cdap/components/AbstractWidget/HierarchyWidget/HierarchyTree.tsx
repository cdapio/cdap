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
import { IHierarchyProps } from 'components/AbstractWidget/HierarchyWidget';
import HierarchyTreeNode from './HierarchyTreeNode/index';

export interface ITreeProps {
  id: number;
  parentId?: number;
  children?: ITreeProps[];
  name: string;
  path?: string[];
  type: string;
}
interface IHierarchyTreeProps extends IHierarchyProps {
  data: ITreeProps[];
  setRecords: (value) => void | React.Dispatch<any>;
  setTree: (value) => void | React.Dispatch<any>;
  records: ITreeProps[];
  dropdownOptions: ITreeProps[];
  disabled: boolean;
}

const HierarchyTree = ({
  data = [],
  records,
  setRecords,
  setTree,
  onChange,
  dropdownOptions,
  disabled,
}: IHierarchyTreeProps) => {
  return (
    <div>
      {data.map((tree: ITreeProps) => (
        <HierarchyTreeNode
          node={tree}
          records={records}
          onChange={onChange}
          setTree={setTree}
          setRecords={setRecords}
          dropdownOptions={dropdownOptions}
          disabled={disabled}
        />
      ))}
    </div>
  );
};

export default HierarchyTree;
