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
import AbstractWidget from './index';
import { withInfo } from '@storybook/addon-info';

export default {
  title: 'Widgets/CodeEditors',
  component: AbstractWidget,
  decorators: [withInfo],
};

export const javascriptEditor = () => <AbstractWidget type="javascript-editor" />;

export const jsonEditor = () => <AbstractWidget type="json-editor" />;

export const pythonEditor = () => <AbstractWidget type="python-editor" />;

export const scalaEditor = () => <AbstractWidget type="scala-editor" />;

export const sqlEditor = () => <AbstractWidget type="sql-editor" />;

export const textEditor = () => <AbstractWidget type="textarea" />;
