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

interface IContextMenuOption {
  name: string;
  label: string;
  onClick: (event: React.SyntheticEvent) => void;
}

interface IContextMenuProps {
  element?: HTMLElement;
  selector?: string;
  options?: IContextMenuOption[];
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

export const ContextMenu = ({ selector, element, options }: IContextMenuProps) => {
  const [mousePosition, setMousePosition] = React.useState(initialMousePosition);

  // state to capture children of context menu to disable right click on them.
  const [children, setChildren] = React.useState(null);
  // we don't use 'useRef' but a 'useCallback' is because 'ref.current' state is not
  // tracked. So a useEffect(() => {...}, [ref.current]) won't get called back if
  // the dom node changes.
  const measuredRef = React.useCallback((node) => {
    if (node !== null) {
      setChildren(Array.prototype.slice.call(node.children));
    }
  }, []);

  React.useEffect(
    () => {
      const defaultEventHandler = (e) => e.preventDefault();
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
    const toggleMenu = (e: PointerEvent) => {
      // setShowMenu((status) => (status ? false : true));
      setMousePosition({
        mouseX: e.clientX - 2,
        mouseY: e.clientY - 4,
      });
      e.preventDefault();
    };

    el.addEventListener('contextmenu', toggleMenu);
    return () => el.removeEventListener('contextmenu', toggleMenu);
  }, []);

  const handleClose = (option: IContextMenuOption, e: React.SyntheticEvent) => {
    setMousePosition(initialMousePosition);
    option.onClick(e);
  };

  return (
    <Menu
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
      {options.map((option) => (
        <StyledMenuItem key={option.name} onClick={handleClose.bind(null, option)}>
          {option.label}
        </StyledMenuItem>
      ))}
    </Menu>
  );
};

export default function ContextMenuWrapper() {
  const options: IContextMenuOption[] = [
    {
      name: 'option1',
      label: 'Option One',
      onClick: (e) => console.log('option 1 clicked'),
    },
    {
      name: 'option2',
      label: 'Option Two',
      onClick: (e) => console.log('option2 clicked'),
    },
    {
      name: 'option3',
      label: 'Option Three',
      onClick: (e) => console.log('option3 clicked'),
    },
  ];
  return (
    <React.Fragment>
      <h1 id="right-click-item">Right click here</h1>
      <ContextMenu selector="#right-click-item" options={options} />
    </React.Fragment>
  );
}
