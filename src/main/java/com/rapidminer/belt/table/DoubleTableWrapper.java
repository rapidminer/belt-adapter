/**
 * Copyright (C) 2001-2021 by RapidMiner and the contributors
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

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.NumericRow;
import com.rapidminer.belt.reader.NumericRowReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.reader.SmallReaders;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.SimpleAttributes;
import com.rapidminer.example.set.HeaderExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.ExampleTable;


/**
 * Wraps a {@link Table} without date-time columns into an {@link ExampleSet} to allow to read it but not to change it.
 * {@link Example} returned by {@link #iterator()} are not to be stored as they change on every call to {@link
 * Iterator#next()}.
 *
 * @author Gisa Meier
 */
public final class DoubleTableWrapper extends RowwiseStatisticsExampleSet {

	private static final long serialVersionUID = 2750264783853433870L;

	/**
	 * {@link Table} is not serializable, but we replace it by an example set on serialization anyway, see
	 * {@link #writeReplace()}.
	 */
	private final transient Table table;
	private final HeaderExampleSet header;
	private final boolean[] nominal;

	/**
	 * Creates a wrapper for a table not containing datetime columns.
	 *
	 * @throws BeltConverter.ConversionException
	 * 		it the table contains non-standard columns
	 */
	DoubleTableWrapper(Table table) {
		this.table = table;
		this.header = getShiftedHeader(table);
		this.nominal = new boolean[table.width()];
		for (int i = 0; i < table.width(); i++) {
			nominal[i] = table.column(i).type().id() == Column.TypeId.NOMINAL;
		}
	}

	public DoubleTableWrapper(DoubleTableWrapper wrapper) {
		this.table = wrapper.table;
		this.header = (HeaderExampleSet) wrapper.header.clone();
		this.nominal = wrapper.nominal;
	}

	@Override
	public Attributes getAttributes() {
		return header.getAttributes();
	}

	@Override
	public int size() {
		return table.height();
	}

	@Override
	public ExampleTable getExampleTable() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Example getExample(int index) {
		NumericRowReader reader = SmallReaders.unbufferedNumericRowReader(table);
		reader.setPosition(index - 1);
		reader.move();
		return new Example(new FakeRow(reader, nominal), header);
	}

	@Override
	public Iterator<Example> iterator() {
		NumericRowReader reader = Readers.numericRowReader(table);
		return new Iterator<Example>() {
			@Override
			public boolean hasNext() {
				return reader.hasRemaining();
			}

			@Override
			public Example next() {
				reader.move();
				return new Example(new FakeRow(reader, nominal), header);
			}
		};
	}


	private static final class FakeRow extends DataRow {
		private static final long serialVersionUID = -2914473986997566956L;
		private final transient NumericRow row;
		private final boolean[] nominal;

		private FakeRow(NumericRow row, boolean[] nominal) {
			this.row = row;
			this.nominal = nominal;
		}

		@Override
		protected double get(int index, double defaultValue) {
			if (nominal[index]) {
				//shift category indices since belt mapping starts with null
				return row.get(index) - 1;
			} else {
				return row.get(index);
			}
		}

		@Override
		protected void set(int index, double value, double defaultValue) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void ensureNumberOfColumns(int numberOfColumns) {
			//nothing to do
		}

		@Override
		public int getType() {
			return DataRowFactory.TYPE_SPECIAL;
		}

		@Override
		public String toString() {
			StringBuilder result = new StringBuilder();
			for (int i = 0; i < row.width(); i++) {
				result.append(i == 0 ? "" : ",").append(row.get(i) - (nominal[i] ? 1 : 0));
			}
			return result.toString();
		}
	}

	/**
	 * When serializing, convert to an example set.
	 *
	 */
	private Object writeReplace() throws ObjectStreamException {
		return BeltConverter.convertSequentially(new IOTable(table));
	}

	/**
	 * This creates a header example set from the table. In contrast to
	 * {@link com.rapidminer.belt.table.BeltConverter#convertHeader(Table)},
	 * the nominal mappings are shifted so that they do not contain {@code null}. This requires an adjustment of the
	 * category indices.
	 *
	 * @param table
	 * 		the table to convert
	 * @return a header example set
	 * @throws BeltConverter.ConversionException
	 * 		it the table contains non-standard columns
	 */
	static HeaderExampleSet getShiftedHeader(Table table) {
		Attributes attributes = new SimpleAttributes();
		List<Attribute> orderedAttributes = new ArrayList<>();
		List<String> labels = table.labels();
		int i = 0;
		for (String label : labels) {
			Column column = table.column(i);
			Attribute attribute = AttributeFactory.createAttribute(label,
					com.rapidminer.belt.table.BeltConverter.getValueType(table, label, i));
			attribute.setTableIndex(i);
			attributes.add(new AttributeRole(attribute));
			orderedAttributes.add(attribute);
			if (attribute.isNominal()) {
				List<String> mapping = ColumnAccessor.get().getDictionaryList(column.getDictionary());
				attribute.setMapping(new ShiftedNominalMappingAdapter(mapping));
			}
			i++;
		}
		BeltConverter.convertRoles(table, attributes);
		HeaderExampleSet exampleSet = new HeaderExampleSet(attributes);
		FromTableConverter.adjustAttributes((Attributes) attributes.clone(), orderedAttributes, exampleSet);
		return exampleSet;
	}

}