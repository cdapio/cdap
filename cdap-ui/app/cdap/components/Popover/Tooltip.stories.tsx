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
import { storiesOf } from '@storybook/react';
import { withInfo } from '@storybook/addon-info';
import { withKnobs, text } from '@storybook/addon-knobs';
import Popover from './index';

storiesOf('Tooltip', module)
  .addDecorator(withKnobs)
  .add(
    'Default Tooltip',
    withInfo({
      text: 'Default tooltip for simple text',
    })(() => [
      <Popover
        targetDimension={{
          margin: '0 5px',
          display: 'inline-block',
        }}
        placement="right"
        target={() => (
          <button className="btn btn-secondary">
            {text('Right Tooltip  Btn', 'Right Tooltip  Btn')}
          </button>
        )}
        showOn="Hover"
        tag="span"
      >
        {text('Right Tooltip  message', 'Right Tooltip  message')}
      </Popover>,
      <Popover
        targetDimension={{
          margin: '0 5px',
          display: 'inline-block',
        }}
        placement="bottom"
        target={() => (
          <button className="btn btn-secondary">
            {text('Bottom Tooltip  Btn', 'Bottom Tooltip  Btn')}
          </button>
        )}
        showOn="Hover"
        tag="span"
      >
        {text('Bottom Tooltip  message', 'Bottom Tooltip  message')}
      </Popover>,
      <div style={{ margin: '5px 0' }}>
        <br />
        <Popover
          targetDimension={{
            margin: '0 5px',
            display: 'inline-block',
          }}
          placement="top"
          target={() => (
            <button className="btn btn-secondary">
              {text('Top Tooltip  Btn', 'Top Tooltip  Btn')}
            </button>
          )}
          showOn="Hover"
          tag="span"
        >
          {text('Top Tooltip  message', 'Top Tooltip  message')}
        </Popover>
        <Popover
          targetDimension={{
            margin: '0 5px',
            display: 'inline-block',
          }}
          placement="left"
          target={() => (
            <button className="btn btn-secondary">
              {text('Left Tooltip  Btn', 'Left Tooltip  Btn')}
            </button>
          )}
          showOn="Hover"
          tag="span"
        >
          {text('Left Tooltip  message', 'Left Tooltip  message')}
        </Popover>
      </div>,
    ])
  );
