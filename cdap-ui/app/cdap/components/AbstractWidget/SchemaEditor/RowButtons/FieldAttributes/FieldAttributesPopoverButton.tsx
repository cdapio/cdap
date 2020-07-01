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
import MoreVertical from '@material-ui/icons/MoreVert';
import { IconWrapper } from 'components/AbstractWidget/SchemaEditor/RowButtons/IconWrapper';
import Popover from '@material-ui/core/Popover';
import makeStyles from '@material-ui/core/styles/makeStyles';
import { ISimpleType, IComplexTypeNames } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { isFlatRowTypeComplex } from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';
import { RecordEnumTypeAttributes } from 'components/AbstractWidget/SchemaEditor/RowButtons/FieldAttributes/RecordEnumAttributes';
import { DecimalTypeAttributes } from 'components/AbstractWidget/SchemaEditor/RowButtons/FieldAttributes/DecimalAttributes';
import If from 'components/If';
import { ITypeProperties } from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import { IOnchangeHandler } from 'components/AbstractWidget/SchemaEditor/EditorTypes';

interface IFieldPropertiesPopoverButtonProps {
  nullable: boolean;
  onNullable: (nullable: boolean) => void;
  type: ISimpleType | IComplexTypeNames;
  onChange?: IOnchangeHandler;
  typeProperties: ITypeProperties;
}

const useAttributePopoverStyles = makeStyles({
  root: {
    marginTop: '15px',
    maxHeight: '250px',
    overflowY: 'auto',
    '& >div': {
      margin: '10px 0',
    },
  },
});

const useStyles = makeStyles((theme) => ({
  popoverContainer: {
    padding: theme.spacing(1),
    width: '300px',
  },
}));

function FieldPropertiesPopoverButton({
  type,
  onChange,
  typeProperties,
}: IFieldPropertiesPopoverButtonProps) {
  const [anchorEl, setAnchorEl] = React.useState(null);

  function handleClick(event) {
    setAnchorEl(event.currentTarget);
  }

  function handleClose() {
    setAnchorEl(null);
  }

  const open = Boolean(anchorEl);
  const id = open ? 'simple-popover' : null;
  const classes = useStyles();
  return (
    <React.Fragment>
      <IconWrapper onClick={handleClick}>
        <MoreVertical />
      </IconWrapper>

      <Popover
        id={id}
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'center',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
      >
        <div className={classes.popoverContainer}>
          <If condition={!isFlatRowTypeComplex(type)}>
            <strong>No Attributes</strong>
          </If>
          <If condition={type === 'record' || type === 'enum'}>
            <strong>Attributes</strong>
            <RecordEnumTypeAttributes
              typeProperties={typeProperties}
              onChange={onChange}
              handleClose={handleClose}
            />
          </If>
          <If condition={type === 'decimal'}>
            <strong>Attributes</strong>
            <DecimalTypeAttributes
              typeProperties={typeProperties}
              onChange={onChange}
              handleClose={handleClose}
            />
          </If>
        </div>
      </Popover>
    </React.Fragment>
  );
}

export { FieldPropertiesPopoverButton, useAttributePopoverStyles };
