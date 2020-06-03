/**
 * Copyright (C) 2001-2020 by RapidMiner and the contributors
 *
 * Complete list of developers available at our web site:
 *
 * http://rapidminer.com
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see
 * http://www.gnu.org/licenses/.
 */
package com.rapidminer.belt.table;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.rapidminer.example.AttributeTypeException;
import com.rapidminer.example.table.NominalMapping;


/**
 * Adapts a {@code List<String>} representing a belt nominal mapping to an immutable {@link NominalMapping}. The {@link
 * NominalMapping} is implemented analogously to {@link com.rapidminer.example.table.PolynominalMapping} but creates the
 * symbol to index mapping only when necessary. In contrast to {@link com.rapidminer.belt.table.NominalMappingAdapter} this class shifts the input
 * mapping by leaving out the {@code null} entry at the first place.
 *
 * @author Gisa Meier
 */
final class ShiftedNominalMappingAdapter implements NominalMapping {

	private static final long serialVersionUID = 1L;

	/**
	 * Error message when trying to modify the mapping.
	 */
	private static final String IMMUTABLE_MAPPING_MESSAGE = "Immutable nominal mapping.";

	private final List<String> mapping;

	private Map<String, Integer> symbolToIndexMap;

	/**
	 * Creates a mapping adapter.
	 *
	 * @param mapping
	 * 		a list that is a belt nominal mapping, in particular starting with {@code null}
	 */
	ShiftedNominalMappingAdapter(List<String> mapping) {
		if (mapping.get(0) != null) {
			throw new IllegalArgumentException("mapping must be a belt mapping");
		}
		//shift the list so that it starts with the element after null
		this.mapping = mapping.subList(1, mapping.size());
	}

	private synchronized void createSymbolToIndexMap() {
		if (symbolToIndexMap == null) {
			symbolToIndexMap = new LinkedHashMap<>();
			for (int i = 0; i < mapping.size(); i++) {
				symbolToIndexMap.put(mapping.get(i), i);
			}
		}
	}

	@Override
	public boolean equals(NominalMapping mapping) {
		if (mapping == this) {
			return true;
		}
		if (this.mapping.size() != mapping.size()) {
			return false;
		}
		if (symbolToIndexMap == null) {
			createSymbolToIndexMap();
		}
		for (String value : mapping.getValues()) {
			if (!symbolToIndexMap.containsKey(value)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Object clone() {
		// mapping is immutable, no need to clone
		return this;
	}

	@Override
	public int getPositiveIndex() {
		ensureClassification();
		if (mapIndex(0) == null) {
			throw new AttributeTypeException("Attribute: Cannot use FIRST_CLASS_INDEX for negative class!");
		}
		if (mapIndex(1) == null) {
			throw new AttributeTypeException("Attribute: No other class than FIRST_CLASS_INDEX found!");
		}
		return 1;
	}

	@Override
	public String getPositiveString() {
		return mapIndex(getPositiveIndex());
	}

	@Override
	public int getNegativeIndex() {
		ensureClassification();
		if (mapIndex(0) == null) {
			throw new AttributeTypeException("Attribute: Cannot use FIRST_CLASS_INDEX for negative class!");
		}
		return 0;
	}

	@Override
	public String getNegativeString() {
		return mapIndex(getNegativeIndex());
	}

	@Override
	public int getIndex(String nominalValue) {
		if (symbolToIndexMap == null) {
			createSymbolToIndexMap();
		}
		Integer index = symbolToIndexMap.get(nominalValue);
		if (index == null) {
			return -1;
		} else {
			return index;
		}
	}

	@Override
	public int mapString(String nominalValue) {
		if (nominalValue == null) {
			return -1;
		}
		int index = getIndex(nominalValue);
		if (index < 0) {
			throw new UnsupportedOperationException(IMMUTABLE_MAPPING_MESSAGE);
		}
		return index;
	}

	@Override
	public String mapIndex(int index) {
		if (index < 0 || index >= mapping.size()) {
			throw new AttributeTypeException(
					"Cannot map index of nominal attribute to nominal value: index " + index + " is out of bounds!");
		}
		return mapping.get(index);
	}

	@Override
	public void setMapping(String nominalValue, int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> getValues() {
		return Collections.unmodifiableList(mapping);
	}

	@Override
	public int size() {
		return mapping.size();
	}

	@Override
	public void sortMappings() {
		throw new UnsupportedOperationException(IMMUTABLE_MAPPING_MESSAGE);
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException(IMMUTABLE_MAPPING_MESSAGE);
	}

	/**
	 * Throws a runtime exception if this attribute is not a classification attribute.
	 *
	 * @see com.rapidminer.example.table.PolynominalMapping
	 */
	private void ensureClassification() {
		if (mapping.size() != 2) {
			throw new AttributeTypeException("Attribute " + this.toString() + " is not a classification attribute!");
		}
	}
}
