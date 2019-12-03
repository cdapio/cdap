/*
 * Copyright © 2019 Cask Data, Inc.
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
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import withStyles from '@material-ui/core/styles/withStyles';
import PropTypes from 'prop-types';
export interface IContextMenuOption {
  name: string;
  label: (() => string) | string;
  onClick: (event: React.SyntheticEvent) => void;
  disabled?: boolean;
}

interface IContextMenuProps {
  element?: HTMLElement;
  selector?: string;
  options?: IContextMenuOption[];
  onOpen?: () => void; // to update disabled flags
}

const initialMousePosition = {
  mouseX: null,
  mouseY: null,
};

const StyledMenuItem = withStyles(() => ({
  root: {
    minHeight: 'auto',
  },
}))(MenuItem);

const StyledDisabledMenuItem = withStyles(() => ({
  root: {
    minHeight: 'auto',
    color: 'lightgrey',
    cursor: 'not-allowed',
  },
}))(MenuItem);

export const ContextMenu = ({ selector, element, options, onOpen }: IContextMenuProps) => {
  const [mousePosition, setMousePosition] = React.useState(initialMousePosition);

  const toggleMenu = (e: PointerEvent) => {
    setMousePosition({
      mouseX: e.clientX - 2,
      mouseY: e.clientY - 4,
    });
    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation();
    if (onOpen) {
      onOpen();
    }
  };
  const defaultEventHandler = (e) => e.preventDefault();

  // state to capture children of context menu to disable right click on them.
  const [children, setChildren] = React.useState(null);
  // we don't use 'useRef' but a 'useCallback' is because 'ref.current' state is not
  // tracked. So a useEffect(() => {...}, [ref.current]) won't get called back if
  // the dom node changes.
  const measuredRef = React.useCallback((node) => {
    if (node !== null) {
      setChildren([...Array.prototype.slice.call(node.children), node]);
    }
  }, []);

  React.useEffect(
    () => {
      if (children) {
        children.forEach((child) => child.addEventListener('contextmenu', defaultEventHandler));
      }
      return () => {
        if (children) {
          children.forEach((child) =>
            child.removeEventListener('contextmenu', defaultEventHandler)
          );
        }
      };
    },
    [children]
  );

  // on mount determine the position of the mouse pointer and place the menu right there.
  React.useEffect(() => {
    let el: HTMLElement;
    if (selector) {
      el = document.querySelector(selector);
    }
    if (element) {
      el = element;
    }
    if (!el) {
      throw new Error("Context Menu either needs a 'selector' or 'element' props to be passed");
    }

    el.addEventListener('contextmenu', toggleMenu);
    return () => el.removeEventListener('contextmenu', toggleMenu);
  }, []);

  const handleClose = (option: IContextMenuOption, e: React.SyntheticEvent) => {
    setMousePosition(initialMousePosition);
    option.onClick(e);
  };

  return (
    <Menu
      keepMounted={true}
      open={mousePosition.mouseY !== null}
      onClose={() => setMousePosition(initialMousePosition)}
      anchorReference="anchorPosition"
      anchorPosition={
        mousePosition.mouseY !== null && mousePosition.mouseX !== null
          ? { top: mousePosition.mouseY, left: mousePosition.mouseX }
          : undefined
      }
      ref={measuredRef}
    >
      {options.map((option) => {
        const { name, label, disabled } = option;
        const MenuItemComp = disabled ? StyledDisabledMenuItem : StyledMenuItem;
        return (
          <MenuItemComp
            key={name}
            onClick={disabled === true ? undefined : handleClose.bind(null, option)}
          >
            {typeof label === 'function' ? label() : label}
          </MenuItemComp>
        );
      })}
    </Menu>
  );
};

(ContextMenu as any).propTypes = {
  selector: PropTypes.string,
  element: PropTypes.node,
  options: PropTypes.object,
  onOpen: PropTypes.func,
};

export default function ContextMenuWrapper() {
  const options: IContextMenuOption[] = [
    {
      name: 'option1',
      label: 'Option One',
      onClick: () => ({}),
    },
    {
      name: 'option2',
      label: 'Option Two',
      onClick: () => ({}),
    },
    {
      name: 'option3',
      label: 'Option Three',
      onClick: () => ({}),
    },
  ];
  return (
    <React.Fragment>
      <h1 id="right-click-item">Right click here</h1>
      <ContextMenu selector="#right-click-item" options={options} />
    </React.Fragment>
  );
}
