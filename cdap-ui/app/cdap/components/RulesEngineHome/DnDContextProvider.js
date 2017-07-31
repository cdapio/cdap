/*
 * Copyright © 2017 Cask Data, Inc.
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

import { DragDropContext } from 'react-dnd';
import HTML5Backend from 'react-dnd-html5-backend';

let defaultManager;

/**
 * This is singleton used to initialize only once dnd in our app.
 * If you initialized dnd and then try to initialize another dnd
 * context the app will break.
 * Here is more info: https://github.com/gaearon/react-dnd/issues/186
 *
 * The solution is to call Dnd context from this singleton this way
 * all dnd contexts in the app are the same.
 */
export default function getDndContextProvider() {
  if (defaultManager) {
    return defaultManager;
  }

  defaultManager = new DragDropContext(HTML5Backend);

  return defaultManager;
}
