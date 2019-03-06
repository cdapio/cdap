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
package co.cask.cdap.common.enums;

/**
 * @author bhupesh.goel
 *
 */
public enum CategoricalColumnEncoding {
    DUMMY_CODING("dummyCoding"), HOT_ENCODING("hotEncoding");
    
    final String name;
    
    CategoricalColumnEncoding(final String name) {
        this.name = name;
    }
    
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
    
    public String getStringEncoding() {
        return "___" + name + "___";
    }
    
    public String getOriginalFeatureName(final String encodedFeature) {
        int index = encodedFeature.indexOf(getStringEncoding());
        if (index >= 0) {
            return encodedFeature.substring(0, index);
        }
        return null;
    }
}
