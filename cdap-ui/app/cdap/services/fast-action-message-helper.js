/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import T from 'i18n-react';

export default function FastActionToMessage(action, options) {
  switch (action) {
    case 'setPreferences':
      return T.translate('features.FastAction.SetPreferences.success', {entityType: options.entityType});
    case 'truncate':
      return T.translate('features.FastAction.truncateSuccess');
    default:
      return '';
  }
}
