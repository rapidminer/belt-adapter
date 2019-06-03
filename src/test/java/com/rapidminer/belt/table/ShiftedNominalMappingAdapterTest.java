/**
 * Copyright (C) 2001-2019 by RapidMiner and the contributors
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.example.AttributeTypeException;
import com.rapidminer.example.table.PolynominalMapping;


/**
 * Tests the {@link com.rapidminer.belt.table.ShiftedNominalMappingAdapter}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ShiftedNominalMappingAdapterTest {

	@RunWith(Parameterized.class)
	public static class Comparison {


		private final com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping;
		private final PolynominalMapping polynominalMapping;

		private static final String ONE = "one";
		private static final String TWO = "two";
		private static final String THREE = "three";
		private static final String NONE = "none";

		public Comparison(String name) {
			List<String> mapping;
			switch (name) {
				case ONE:
					mapping = Arrays.asList(null, "one");
					break;
				case TWO:
					mapping = Arrays.asList(null, "one", "two");
					break;
				case THREE:
					mapping = Arrays.asList(null, "one", "two", "three");
					break;
				default:
					mapping = Collections.singletonList(null);
					break;
			}
			adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(mapping);
			polynominalMapping = getPolynomialMapping(mapping);
		}

		@Parameterized.Parameters(name = "mapping_{0}")
		public static Collection<Object> params() {
			return Arrays.asList(ONE, TWO, THREE, NONE);
		}

		private PolynominalMapping getPolynomialMapping(List<String> list) {
			Map<Integer, String> map = new LinkedHashMap<>();
			for (int i = 1; i < list.size(); i++) {
				map.put(i - 1, list.get(i));
			}
			return new PolynominalMapping(map);
		}


		@Test
		public void testSize() {
			assertEquals(polynominalMapping.size(), adaptedMapping.size());
		}

		@Test
		public void testGetValues() {
			assertEquals(polynominalMapping.getValues(), adaptedMapping.getValues());
		}

		@Test
		public void testMapIndex() {
			for (int i = 0; i < adaptedMapping.size(); i++) {
				assertEquals(polynominalMapping.mapIndex(i), adaptedMapping.mapIndex(i));
			}
		}

		@Test
		public void testGetIndex() {
			for (String string : Arrays.asList("one", "two", "three")) {
				assertEquals(polynominalMapping.getIndex(string), adaptedMapping.getIndex(string));
			}
		}

		@Test
		public void testMapString() {
			assertEquals(polynominalMapping.mapString(null), adaptedMapping.mapString(null));
			if (adaptedMapping.size() > 1) {
				assertEquals(polynominalMapping.mapString("one"), adaptedMapping.mapString("one"));
			}
		}

		@Test
		public void testEquals() {
			assertTrue(adaptedMapping.equals(polynominalMapping));
			assertTrue(polynominalMapping.equals(adaptedMapping));
			assertTrue(adaptedMapping.equals(adaptedMapping));
		}

		@Test
		public void testClone() {
			assertEquals(adaptedMapping, adaptedMapping.clone());
		}
	}


	public static class Input {

		@Test(expected = AttributeTypeException.class)
		public void testMapIndexNegative() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			adaptedMapping.mapIndex(-1);
		}

		@Test(expected = AttributeTypeException.class)
		public void testMapIndexBigger() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			adaptedMapping.mapIndex(3);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testMapStringNotContained() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			adaptedMapping.mapString("four");
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testSetMapping() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			adaptedMapping.setMapping("val", 1);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testSortMapping() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			adaptedMapping.sortMappings();
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testClear() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			adaptedMapping.clear();
		}

		@Test
		public void testGetPositiveIndex() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			assertEquals(1, adaptedMapping.getPositiveIndex());
		}

		@Test
		public void testGetNegativeIndex() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			assertEquals(0, adaptedMapping.getNegativeIndex());
		}

		@Test(expected = AttributeTypeException.class)
		public void testNoNegativeIndexSize() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one"));
			adaptedMapping.getNegativeIndex();
		}

		@Test(expected = AttributeTypeException.class)
		public void testNoNegativeIndex() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, null, null));
			adaptedMapping.getNegativeIndex();
		}

		@Test(expected = AttributeTypeException.class)
		public void testNoPositiveIndex() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", null));
			adaptedMapping.getPositiveIndex();
		}

		@Test(expected = AttributeTypeException.class)
		public void testNoPositiveIndexNoNegative() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, null, null));
			adaptedMapping.getPositiveIndex();
		}

		@Test
		public void testGetPositiveString() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			assertEquals("two", adaptedMapping.getPositiveString());
		}

		@Test
		public void testGetNegativeString() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			assertEquals("one", adaptedMapping.getNegativeString());
		}

		@Test
		public void testNotEqualsSize() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			assertFalse(adaptedMapping.equals(new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one"))));
		}

		@Test
		public void testNotEqualsDifferentValue() {
			com.rapidminer.belt.table.ShiftedNominalMappingAdapter adaptedMapping = new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "two"));
			assertFalse(adaptedMapping.equals(new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList(null, "one", "three"))));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNotBeltMapping() {
			new com.rapidminer.belt.table.ShiftedNominalMappingAdapter(Arrays.asList("one", "two"));
		}
	}
}
