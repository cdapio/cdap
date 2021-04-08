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
import { makeStyles, Theme, withStyles } from '@material-ui/core/styles';
import Box from '@material-ui/core/Box';
import { Paper, TextField } from '@material-ui/core';
import DeleteIcon from '@material-ui/icons/Delete';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import KeyboardArrowRightIcon from '@material-ui/icons/KeyboardArrowRight';
import Autocomplete from '@material-ui/lab/Autocomplete';
import { toJsonHandler, getChildren, flatToTree, removeFamily } from '../dataHandler';
import { IHierarchyProps } from 'components/AbstractWidget/HierarchyWidget';
import HierarchyTreeLeaf from 'components/AbstractWidget/HierarchyWidget/HierarchyTreeNode/HierarchyTreeLeaf';
import HierarchyPopoverButton from 'components/AbstractWidget/HierarchyWidget/HierarchyTreeNode/PopoverButton';
import InputFieldWrapper from 'components/AbstractWidget/HierarchyWidget/HierarchyTreeNode/InputFieldWrapper';
import ListboxComponent from 'components/AbstractWidget/HierarchyWidget/HierarchyTreeNode/ListBoxComponent';
import HierarchyTree from 'components/AbstractWidget/HierarchyWidget/HierarchyTree';
import If from 'components/If';
import uuidV4 from 'uuid/v4';

const RowInputContainer = withStyles(() => {
  return {
    root: {
      padding: '2px 10px 2px 0px',
      display: 'grid',
      marginTop: '2px',
      gridTemplateColumns: '4fr 2fr 0fr',
      alignItems: 'center',
      gridTemplateRows: '28px',
      position: 'relative',
      borderRadius: '3px',
    },
  };
})(Paper);

const AutocompleteContainer = withStyles(() => {
  return {
    root: {
      padding: '2px 10px 2px 0px',
      marginTop: '2px',
      position: 'relative',
      borderRadius: '3px',
    },
  };
})(Paper);

const RowButtonsWraper = withStyles(() => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '1fr 1fr',
      justifyItems: 'end',
    },
  };
})(Box);

interface IHierarchyTreeNodeStyleProps {
  marginLeft: number;
  height: number;
}

const useStyles = makeStyles<Theme, IHierarchyTreeNodeStyleProps>(() => {
  return {
    listbox: {
      maxHeight: 'min-content',
    },
    hierarchyIcon: {
      width: '10px',
      height: (props) => `${props.height}px`,
      position: 'absolute',
      borderLeft: '2px solid rgba(0, 0, 0, 0.2)',
      left: '-10px',
    },
    innerMostSiblingConnector: {
      top: '17px',
      left: '-8px',
      width: '8px',
      height: '2px',
      content: '""',
      position: 'absolute',
      borderTop: '2px solid rgba(0, 0, 0, 0.2)',
    },
    hierarchyTreeMargin: {
      marginLeft: (props) => props.marginLeft,
    },
    multiSelectCustomMargin: {
      marginLeft: (props) => (props.marginLeft === 0 ? `10px` : `${props.marginLeft}px`),
    },
    relative: {
      position: 'relative',
    },
    checkBox: {
      display: 'flex',
      width: '100%',
    },
    paddingTextField: {
      paddingLeft: '10px',
    },
    buttonIcon: {
      padding: '6.5px',
    },
    hiddenIcon: {
      padding: '6.5px',
      visibility: 'hidden',
    },
  };
});

interface INodeProps {
  id: number;
  parentId?: number;
  children?: INodeProps[];
  name: string;
  path?: string[];
  type: string;
}
interface IHierarchyTreeNodeProps extends IHierarchyProps {
  node: INodeProps;
  setRecords: (value) => void | React.Dispatch<any>;
  setTree: (value) => void | React.Dispatch<any>;
  records: INodeProps[];
  dropdownOptions: INodeProps[];
  disabled: boolean;
}

const HierarchyTreeNode = ({
  node,
  setRecords,
  setTree,
  onChange,
  records,
  dropdownOptions,
  disabled,
}: IHierarchyTreeNodeProps) => {
  const [multiSelectOptions, setMultiSelectOptions] = React.useState([]);
  const [childVisible, setChildVisiblity] = React.useState(false);
  const [showMultiSelect, setShowMultiSelect] = React.useState(false);
  const hasChild = node.children.length !== 0 ? true : false;

  let mrgL = 0;
  let heightVal = 34;
  if (hasChild && childVisible) {
    mrgL += 10;
    heightVal = (node.children.length + 1) * 33.6;
  }

  const classes = useStyles({ marginLeft: mrgL, height: heightVal });
  const autoCompleteClass = useStyles({ marginLeft: mrgL, height: 34 });

  const fillMultiSelectOptions = (e) => {
    setMultiSelectOptions(e);
  };

  const handleMultiSelectClose = () => {
    const newRecords = [...records];
    multiSelectOptions.forEach((item) => {
      let childrenOfSelected = [];
      const uniqueId = uuidV4();

      if (item.children.length !== 0) {
        childrenOfSelected = getChildren(childrenOfSelected, item.children, uniqueId, []);
      }
      const childToBeAdded = {
        id: uniqueId,
        path: item.path,
        parentId: node.id,
        name: item.name,
        children: [],
        type: item.type,
      };
      setShowMultiSelect(false);
      setChildVisiblity(true);
      newRecords.push(childToBeAdded);
      if (childrenOfSelected) {
        childrenOfSelected.forEach((child) => {
          newRecords.push(child);
        });
      }
    });
    setTree(flatToTree(newRecords));
    onChange(JSON.stringify(toJsonHandler(newRecords)));
    setRecords(newRecords);
    setMultiSelectOptions([]);
  };

  const nameChangeHandler = (e) => {
    const newRecords = [...records];
    newRecords.forEach((record) => {
      if (record.id === node.id) {
        record.name = e.target.value;
      }
    });
    setTree(flatToTree(newRecords));
    onChange(JSON.stringify(toJsonHandler(newRecords)));
    setRecords(newRecords);
  };

  const handleAddNewRecordToParent = () => {
    const newRecords = [...records];
    newRecords.push({
      id: uuidV4(),
      parentId: node.id,
      type: 'record',
      children: [],
      name: '',
    });
    setTree(flatToTree(newRecords));
    setRecords(newRecords);
    setChildVisiblity(true);
  };

  const handleDeleteElement = () => {
    const newRecords = [...records];
    const elements = removeFamily(node.id, newRecords);
    setTree(flatToTree(elements));
    onChange(JSON.stringify(toJsonHandler(elements)));
    setRecords(elements);
  };

  const ArrowIcon = (
    <IconButton
      className={classes.buttonIcon}
      onClick={() => {
        setChildVisiblity((value) => !value);
        setShowMultiSelect(false);
      }}
    >
      {childVisible ? <KeyboardArrowDownIcon /> : <KeyboardArrowRightIcon />}
    </IconButton>
  );

  return (
    <React.Fragment>
      <If condition={node.parentId !== null}>
        <div className={classes.relative}>
          <div className={classes.hierarchyIcon} />
          <div className={classes.innerMostSiblingConnector} />
        </div>
      </If>
      <RowInputContainer>
        <InputFieldWrapper
          node={node}
          nameChangeHandler={nameChangeHandler}
          arrowIcon={ArrowIcon}
          disabled={disabled}
        />
        <span>{node.type || ''}</span>
        <RowButtonsWraper>
          {!node.path ? (
            <IconButton disabled={disabled} className={classes.buttonIcon}>
              <HierarchyPopoverButton
                setShowMultiSelect={() => {
                  setChildVisiblity(true);
                  setShowMultiSelect(true);
                }}
                addNewRecordToParent={() => handleAddNewRecordToParent()}
              />
            </IconButton>
          ) : (
            <IconButton className={classes.hiddenIcon}>
              <KeyboardArrowRightIcon />
            </IconButton>
          )}
          <IconButton
            onClick={handleDeleteElement}
            disabled={disabled}
            className={classes.buttonIcon}
          >
            <DeleteIcon data-cy="remove" />
          </IconButton>
        </RowButtonsWraper>
      </RowInputContainer>
      <If condition={childVisible}>
        <div className={classes.hierarchyTreeMargin}>
          <HierarchyTree
            records={records}
            data={node.children}
            onChange={onChange}
            setTree={setTree}
            setRecords={setRecords}
            dropdownOptions={dropdownOptions}
            disabled={disabled}
          />
        </div>
      </If>
      <If condition={showMultiSelect}>
        <div className={classes.multiSelectCustomMargin}>
          <div className={classes.relative}>
            <div className={autoCompleteClass.hierarchyIcon} />
            <div className={classes.innerMostSiblingConnector} />
          </div>
          <AutocompleteContainer>
            <Autocomplete
              classes={{ listbox: classes.listbox }}
              multiple
              disableCloseOnSelect
              options={dropdownOptions}
              value={multiSelectOptions}
              onChange={(_, value: string[]) => fillMultiSelectOptions(value)}
              getOptionSelected={(a, b) => a.id === b.id}
              onClose={handleMultiSelectClose}
              ListboxComponent={(listboxProps) => <ListboxComponent {...listboxProps} />}
              getOptionLabel={(option) => option.name}
              renderOption={(option, { selected }) => {
                return (
                  <div className={classes.checkBox}>
                    <HierarchyTreeLeaf option={option} selected={selected} />
                  </div>
                );
              }}
              renderInput={(params) => (
                <TextField
                  className={classes.paddingTextField}
                  {...params}
                  InputProps={{ ...params.InputProps, disableUnderline: true }}
                />
              )}
            />
          </AutocompleteContainer>
        </div>
      </If>
    </React.Fragment>
  );
};

export default HierarchyTreeNode;
