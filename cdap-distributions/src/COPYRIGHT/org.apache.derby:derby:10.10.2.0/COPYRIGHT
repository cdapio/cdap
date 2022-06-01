/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package javax.jdo;

/**
 * Converts persistent attribute values (fields or properties) to different
 * values stored in the underlying datastore and vice versa.
 * [TBD: 
 * <ul>
 * <li>should we require that converters need access to any other information, e.g metadata? passed into the constructor</li>
 * <li>otherwise we assume there is a default constructor, and is instantiable using the current JDO class loader(s)</li>
 * </ul>]
 * 
 * @param <A> The type of persistent attribute (field or property).
 * @param <D> The type to be used in the datastore.
 */
public interface AttributeConverter<A, D> {

    /**
     * Converts the given persistent attribute value to its representation in the datastore.
     * @param attributeValue Value of attribute
     * @return Value in datastore
     */
    D convertToDatastore(A attributeValue);

    /**
     * Converts the given datastore value to its representation as a persistent attribute.
     * @param datastoreValue Value in datastore
     * @return Value in attribute
     */
    A convertToAttribute(D datastoreValue);

    /**
     * Dummy converter to represent "use the implementation default for this type" when using annotations.
     */
	public static class UseDefault implements AttributeConverter<Object, Object>
	{
		public Object convertToDatastore(Object attributeValue) {
			throw new JDOUserException("This converter is not usable.");
		}

		public Object convertToAttribute(Object datastoreValue) {
			throw new JDOUserException("This converter is not usable.");
		}
	}
}
