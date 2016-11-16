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
import WranglerActions from 'components/Wrangler/Redux/WranglerActions';
import WranglerStore from 'components/Wrangler/Redux/WranglerStore';

export default function UpperCaseAction({column}) {
  function onClick() {
    WranglerStore.dispatch({
      type: WranglerActions.upperCaseColumn,
      payload: {
        activeColumn: column
      }
    });
  }

  return (
    <span className="column-actions">
      <span onClick={onClick}>T</span>
    </span>
  );
}

UpperCaseAction.propTypes = {
  column: PropTypes.string
};
