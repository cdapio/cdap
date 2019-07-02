/*
 * Copyright © 2018 Cask Data, Inc.
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
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import classnames from 'classnames';

import './ActionsPopover.scss';

export interface IAction {
  readonly label: string | React.ReactNode | 'separator';
  actionFn?: () => void;
  disabled?: boolean;
  className?: string;
  title?: string;
  link?: string;
}

interface IActionsPopoverProps {
  actions: IAction[];
}

const POPPER_MODIFIERS = {
  preventOverflow: {
    enabled: false,
  },
  hide: {
    enabled: false,
  },
};

const ActionsPopover: React.SFC<IActionsPopoverProps> = ({ actions }) => {
  return (
    <Popover
      target={(props) => <IconSVG name="icon-cog-empty" {...props} />}
      className="actions-popover"
      placement="bottom"
      bubbleEvent={true}
      enableInteractionInPopover={true}
      modifiers={POPPER_MODIFIERS}
    >
      <ul>
        {actions.map((action, i) => {
          if (action.label === 'separator') {
            return <hr key={`${action.label}${i}`} />;
          }

          const onClick = () => {
            if (action.disabled || !action.actionFn) {
              return;
            }
            action.actionFn();
          };

          if (action.link) {
            return (
              <li key={action.className} title={action.label.toString()}>
                <a href={action.link} target="_blank">
                  {action.label}
                </a>
              </li>
            );
          }

          return (
            <li
              key={i}
              className={classnames(action.className, { disabled: action.disabled })}
              onClick={onClick}
              title={action.title}
            >
              {action.label}
            </li>
          );
        })}
      </ul>
    </Popover>
  );
};

export default ActionsPopover;
