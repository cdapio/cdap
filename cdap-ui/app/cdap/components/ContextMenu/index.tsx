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

export const ContextMenu = ({ selector, element, options }: IContextMenuProps) => {
  const [mousePosition, setMousePosition] = React.useState(initialMousePosition);
  const menuRef = React.useRef(null);
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

  React.useEffect(
    () => {
      const defaultEventHandler = (e) => e.preventDefault();
      if (menuRef.current) {
        Array.prototype
          .slice(menuRef.current.children)
          .forEach((child) => child.addEventListener('contextmenu', defaultEventHandler));
      }
      return () => {
        if (menuRef.current) {
          Array.prototype
            .slice(menuRef.current.children)
            .forEach((child) => child.removeEventListener('contextmenu', defaultEventHandler));
        }
      };
    },
    [menuRef.current]
  );

  const handleClose = () => {
    setMousePosition(initialMousePosition);
  };

  return (
    <Menu
      open={mousePosition.mouseY !== null}
      onClose={handleClose}
      anchorReference="anchorPosition"
      anchorPosition={
        mousePosition.mouseY !== null && mousePosition.mouseX !== null
          ? { top: mousePosition.mouseY, left: mousePosition.mouseX }
          : undefined
      }
      ref={menuRef}
    >
      <MenuItem onClick={handleClose}>Copy</MenuItem>
      <MenuItem onClick={handleClose}>Print</MenuItem>
      <MenuItem onClick={handleClose}>Highlight</MenuItem>
      <MenuItem onClick={handleClose}>Email</MenuItem>
    </Menu>
  );
};

export default function ContextMenuWrapper() {
  return (
    <React.Fragment>
      <h1 id="right-click-item">Right click here</h1>
      <ContextMenu selector="#right-click-item" />
    </React.Fragment>
  );
}
