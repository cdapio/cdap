/*
 * Copyright © 2016 Cask Data, Inc.
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
import React from 'react';
import { mount } from 'enzyme';
import SchemaEditor from 'components/SchemaEditor';

describe('Unit Tests for Schema Editor', () => {
  it('Should render', () => {
    const schemaeditor = mount(<SchemaEditor />);
    expect(schemaeditor.find('.schema-body').length).toBe(1);
  });
  it('Should render a single empty RecordSchemaRow', () => {
    const schemaeditor = mount(<SchemaEditor />);
    expect(schemaeditor.find('.record-schema-row').length).toBe(1);
    expect(schemaeditor.find('.record-schema-row > .schema-row').length).toBe(1);
  });
});
