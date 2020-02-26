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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import { IContextMenuOption } from 'components/ContextMenu';
const menuItemStyles = () => {
  return {
    menuItemLabel: {
      verticalAlign: 'middle',
      margin: '0 5px',
    },
  };
};
interface IMenuItemContentProps extends WithStyles<typeof menuItemStyles> {
  option: IContextMenuOption;
}

const MenuItemContent = ({ classes, option }: IMenuItemContentProps) => {
  const { icon, label } = option;
  return (
    <div>
      <If condition={icon && icon.toString() ? true : false}>{icon}</If>
      <span className={classes.menuItemLabel}>{typeof label === 'function' ? label() : label}</span>
    </div>
  );
};

const StyledMenuItemContent = withStyles(menuItemStyles)(MenuItemContent);
export default StyledMenuItemContent;
