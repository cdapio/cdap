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

import React, {PropTypes} from 'react';
import { Input } from 'reactstrap';
// WE NEED OBJECT SPREAD OPERATOR
export default function InputWithValidations({
  defaultValue,
  value,
  type,
  placeholder,
  onChange,
  validationError
}) {
  return (
    <div>
      <Input
        defaultValue={defaultValue}
        value={value}
        type={type}
        onChange={onChange}
        placeholder={placeholder}
      />
      <span className="text-danger">{validationError}</span>
    </div>
  );
}
InputWithValidations.propTypes = {
  type: PropTypes.string,
  size: PropTypes.string,
  placeholder: PropTypes.string,
  defaultValue: PropTypes.string,
  value: PropTypes.string,
  className: PropTypes.string,
  validationError: PropTypes.string,
  onChange: PropTypes.func
};
