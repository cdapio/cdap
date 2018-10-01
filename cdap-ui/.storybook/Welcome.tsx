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

import * as React from "react";
import { storiesOf } from '@storybook/react';
const cdaplogo = require('../app/cdap/styles/img/company_logo.png');
import { withInfo } from '@storybook/addon-info';

// Eventually all the style attributes will be changed to styled-components
// Since this is a one-of component meh.
export default function Welcome() {
  return (
    <div style={{
      height: '100vh',
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'center',
      alignItems: 'center',
      background: '#3b78e7',
      color: 'white',
    }}>
      <img src={cdaplogo} />
      <h1>😍 Welcome to CDAP UI storybook!</h1>
      <p>
        Repository of components used in CDAP UI
      </p>
      <img src="https://i2.wp.com/blog.codepen.io/wp-content/uploads/2018/07/happy-michael-scott.gif" />
    </div>
  );
}

storiesOf('Welcome', module)
  .add(
    'with text',
    withInfo({
      text: 'Welcome page',
    })(() => (<Welcome />)));
