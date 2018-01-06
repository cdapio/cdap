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

const REGRESSION_ALGORITHMS = [
  {
    name: 'linear.regression',
    label: 'Linear Regression'
  },
  {
    name: 'generalized.linear.regression',
    label: 'Generalized Linear Regression'
  },
  {
    name: 'decision.tree.regression',
    label: 'Decision Tree Regression'
  },
  {
    name: 'random.forest.regression',
    label: 'Random Forest Regression'
  },
  {
    name: 'gradient.boosted.tree.regression',
    label: 'Gradient Boosted Tree Regression'
  }
];
const CLASSIFIER_ALGORITHMS = [
  {
    name: 'decision.tree.classifier',
    label: 'Decision Tree Classifier'
  },
  {
    name: 'random.forest.classifier',
    label: 'Random Forest Classifier'
  },
  {
    name: 'gradient.boosted.tree.classifier',
    label: 'Gradient Boosted Tree Classifier'
  },
  {
    name: 'multilayer.perceptron.classifier',
    label: 'Multilayer Perceptron Classifier'
  },
  {
    name: 'logistic.regression',
    label: 'Logistic Regression'
  },
  {
    name: 'naive.bayes',
    label: 'Naive Bayes'
  }
];
export {REGRESSION_ALGORITHMS, CLASSIFIER_ALGORITHMS};
