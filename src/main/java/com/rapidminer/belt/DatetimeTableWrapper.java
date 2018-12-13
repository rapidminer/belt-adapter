/**
 * Copyright (C) 2001-2018 by RapidMiner and the contributors
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
package com.rapidminer.belt;

import java.io.ObjectStreamException;
import java.time.Instant;
import java.util.Iterator;

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.set.HeaderExampleSet;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.ExampleTable;


/**
 * Wraps a {@link Table} with date-time columns into an {@link ExampleSet} to allow to read it but not to change it.
 * {@link Example} returned by {@link #iterator()} are not to be stored as they change on every call to {@link
 * Iterator#next()}.
 *
 * @author Gisa Meier
 */
public final class DatetimeTableWrapper extends RowwiseStatisticsExampleSet {

	private static final long serialVersionUID = 548442173952040494L;

	private enum ReadType {
		NUMERIC, NOMINAL, DATETIME;
	}

	/**
	 * Function from {@link MixedRow} and int to double.
	 */
	@FunctionalInterface
	private interface ToDoubleIntRowFunction {

		double apply(MixedRow row, int index);
	}

	/**
	 * Reading function for numeric columns.
	 */
	private static final ToDoubleIntRowFunction NUMERIC = MixedRow::getNumeric;

	/**
	 * Reading function for date-time columns.
	 */
	private static double getDateTime(MixedRow row, int index) {
		Instant instant = (Instant) row.getObject(index);
		return instant == null ? Double.NaN : instant.toEpochMilli();
	};

	/**
	 * {@link Table} is not serializable, but we replace it by an example set on serialization anyway, see
	 * {@link #writeReplace()}.
	 */
	private final transient Table table;
	private final HeaderExampleSet header;
	private final ReadType[] readTypes;

	DatetimeTableWrapper(Table table) {
		this.table = table;
		this.header = DoubleTableWrapper.getShiftedHeader(table);
		readTypes = new ReadType[table.width()];
		for (int i = 0; i < table.width(); i++) {
			if (table.column(i).type().id() == Column.TypeId.DATE_TIME) {
				readTypes[i] = ReadType.DATETIME;
			} else if (table.column(i).type().id() == Column.TypeId.NOMINAL) {
				readTypes[i] = ReadType.NOMINAL;
			} else {
				readTypes[i] = ReadType.NUMERIC;
			}
		}
	}


	public DatetimeTableWrapper(DatetimeTableWrapper wrapper) {
		this.table = wrapper.table;
		this.readTypes = wrapper.readTypes;
		this.header = (HeaderExampleSet) wrapper.header.clone();
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
		MixedRowReader reader = new MixedRowReader(table.getColumns(), NumericReader.MIN_BUFFER_SIZE);
		reader.setPosition(index - 1);
		reader.move();
		return new Example(new FakeRow(reader, readTypes), header);
	}

	@Override
	public Iterator<Example> iterator() {
		MixedRowReader reader = new MixedRowReader(table.getColumns());
		return new Iterator<Example>() {
			@Override
			public boolean hasNext() {
				return reader.hasRemaining();
			}

			@Override
			public Example next() {
				reader.move();
				return new Example(new FakeRow(reader, readTypes), header);
			}
		};
	}

	private static final class FakeRow extends DataRow {

		private static final long serialVersionUID = -4422364455662199363L;
		private final transient MixedRow row;
		private final transient ReadType[] readTypes;

		private FakeRow(MixedRow row, ReadType[] readTypes) {
			this.row = row;
			this.readTypes = readTypes;
		}

		@Override
		protected double get(int index, double defaultValue) {
			switch (readTypes[index]) {
				case NOMINAL:
					//shift category indices since belt mapping starts with null
					return row.getNumeric(index) - 1;
				case DATETIME:
					return getDateTime(row, index);
				case NUMERIC:
				default:
					return row.getNumeric(index);
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
				result.append(i == 0 ? "" : ",").append(get(i, 0));
			}
			return result.toString();
		}
	}

	/**
	 * When serializing, convert to an example set.
	 */
	private Object writeReplace() throws ObjectStreamException {
		return BeltConverter.convertSequentially(new IOTable(table));
	}
}