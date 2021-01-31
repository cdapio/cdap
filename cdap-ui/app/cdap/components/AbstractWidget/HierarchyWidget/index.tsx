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
import IconButton from '@material-ui/core/IconButton';
import { withStyles } from '@material-ui/core/styles';
import AddIcon from '@material-ui/icons/Add';
import HierarchyTree from './HierarchyTree';
import uuidV4 from 'uuid/v4';
import { objectQuery } from 'services/helpers';
import { flatToTree, toJsonHandler, jsonToFlat, getFields, inputSchemaToFlat } from './dataHandler';
import Heading, { HeadingTypes } from 'components/Heading';

export const IconWrapper = withStyles(() => {
  return {
    root: {
      borderRadius: 10,
      padding: '5px',
      marginTop: '5px',
      '&:focus': {
        outline: 'none',
      },
    },
  };
})(IconButton);

interface IHierarchyWidgetProps {
  allowedTypes?: string[];
}
export interface IStageSchema {
  name: string;
  schema: string;
}
export interface IWidgetExtraConfig {
  namespace?: string;
  inputSchema?: IStageSchema[];
}
export interface IHierarchyProps<T = any> extends IHierarchyWidgetProps {
  value?: string;
  widgetProps?: T;
  onChange?: (value) => void | React.Dispatch<any>;
  extraConfig?: IWidgetExtraConfig;
  disabled?: boolean;
}

const HierarchyWidget = ({
  value,
  onChange,
  extraConfig,
  widgetProps,
  disabled,
}: IHierarchyProps) => {
  const [records, setRecords]: any = React.useState([]);
  const [tree, setTree] = React.useState([]);
  const inputSchema = objectQuery(extraConfig, 'inputSchema') || [];
  const allowedTypes = widgetProps.allowedTypes || [];
  const fieldValues = getFields(inputSchema, allowedTypes);
  const dropdownData = inputSchemaToFlat(fieldValues);
  const dropdownOptions = [];
  const arrayOfIds = [];

  dropdownData.forEach((item) => {
    if (item.type !== 'record' && item.type !== 'string') {
      arrayOfIds.push(item.id);
    }
  });

  dropdownData.forEach((item) => {
    const element = arrayOfIds.find((el) => el === item.parentId);
    if (!element) {
      dropdownOptions.push(item);
    }
  });

  React.useEffect(() => {
    if (value) {
      try {
        const responseParsed = JSON.parse(value);
        if (responseParsed.length !== 0) {
          const jsonToFlatData = jsonToFlat(responseParsed);
          jsonToFlatData.map((record) => {
            if (record.path) {
              const el = dropdownData.find((item) => item.name === record.name);
              record.type = el.type;
            }
          });
          setTree(flatToTree(jsonToFlatData));
          setRecords(jsonToFlatData);
        }
      } catch (err) {
        // tslint:disable-next-line: no-console
        console.log('Users using fields that are not in the input schema.', err);
      }
    }
  }, []);

  return (
    <div>
      <Heading type={HeadingTypes.h6} label={'Organize field under new or existing records'} />
      <Heading type={HeadingTypes.h6} label={records.length + ' records'} />
      <HierarchyTree
        records={records}
        data={tree}
        onChange={onChange}
        setTree={setTree}
        setRecords={setRecords}
        dropdownOptions={dropdownOptions}
        disabled={disabled}
      />
      <IconWrapper
        disabled={disabled}
        onClick={() => {
          const newRecords = [...records];
          newRecords.push({
            id: uuidV4(),
            parentId: null,
            type: 'record',
            children: [],
            name: '',
          });
          setTree(flatToTree(newRecords));
          onChange(JSON.stringify(toJsonHandler(newRecords)));
          setRecords(newRecords);
        }}
      >
        <AddIcon />
        <Heading type={HeadingTypes.h6} label="ADD RECORD" data-cy="add" />
      </IconWrapper>
    </div>
  );
};

export default HierarchyWidget;
