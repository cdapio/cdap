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

const iconMap = {
  create_app: 'icon-app',
  create_pipeline: 'icon-pipelines',
  create_pipeline_draft: 'icon-pipelines',
  create_artifact: 'icon-artifacts',
  informational: 'icon-info',
  load_datapack: 'icon-upload',
  load_app: 'icon-upload',
  __default__: 'icon-tasks',
};

const getIcon = (action) => {
  return iconMap[action] ? iconMap[action] : iconMap['__default__'];
};

export default getIcon;
export { iconMap };
