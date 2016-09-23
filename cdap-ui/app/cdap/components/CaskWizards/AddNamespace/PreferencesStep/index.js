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
import {Col, FormGroup, Label, Form} from 'reactstrap';
require('./PreferencesStep.less');

export default function PreferencesStep() {
  return(
      <Form
        className="form-horizontal"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
        <FormGroup>
          <Col xs="2">
            <Label>Format:</Label>
          </Col>
          <Col xs="4">
            <input type="text" />
          </Col>
        </FormGroup>
        <FormGroup>
          <Col xs="2">
            <Label>Schema:</Label>
          </Col>
          <Col xs="4">
            <input type="text" />
          </Col>
        </FormGroup>
      </Form>
  );
}
