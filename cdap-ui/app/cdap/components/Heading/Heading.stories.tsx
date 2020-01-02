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
import Heading, { HeadingTypes } from './index';
import { withKnobs, text } from '@storybook/addon-knobs';

storiesOf('Heading', module)
  .addDecorator(withKnobs)
  .add(
    'H1-6 Headings',
    withInfo({
      text: 'Render H1-6 headings',
    })(() => (
      <div>
        <Heading type={HeadingTypes.h1} label={text('H1 Heading', 'H1 Heading')} />
        <Heading type={HeadingTypes.h2} label={text('H2 Heading', 'H2 Heading')} />
        <Heading type={HeadingTypes.h3} label={text('H3 Heading', 'H3 Heading')} />
        <Heading type={HeadingTypes.h4} label={text('H4 Heading', 'H4 Heading')} />
        <Heading type={HeadingTypes.h5} label={text('H5 Heading', 'H5 Heading')} />
        <Heading type={HeadingTypes.h6} label={text('H6 Heading', 'H6 Heading')} />
      </div>
    ))
  );
