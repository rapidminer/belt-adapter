/**
 * Copyright (C) 2001-2020 by RapidMiner and the contributors
 *
 * Complete list of developers available at our web site:
 *
 * http://rapidminer.com
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
package com.rapidminer.belt.table;

import java.lang.ref.WeakReference;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.SmallReaders;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.tools.container.Pair;
import com.rapidminer.tools.container.Triple;


/**
 * {@link AbstractTableAccessor} for the case that there are date-time columns, i.e. not everything can be read
 * numerically. While reading a whole table would be normally an use case for a {@link
 * com.rapidminer.belt.reader.MixedRowReader}, benchmarks have shown that using a row reader has horrible performance
 * when the table is read column-wise. Therefore, we use an array of column readers which make column-wise reading much
 * faster and row-wise reading only slightly slower.
 * <p>
 * Since the date-time columns require a {@link ObjectReader} while the others must be read with a {@link
 * NumericReader}, we first create one list of the date-time columns and one for the others together with a twist-array
 * which maps the original column index to the new index in the table where date-time columns come last.
 *
 * @author Gisa Meier
 * @since 0.7
 */
class MixedTableAccessor extends AbstractTableAccessor {

	/**
	 * {@link ThreadLocal} with a weak reference that holds the array of numeric readers. The {@link WeakReference} is
	 * needed since otherwise the readers and thus all columns are kept as long as the thread is alive even if this
	 * {@link MixedTableAccessor} is already gone. With the weak reference, we might have to recreate the readers in
	 * case of garbage collection but this is better than the alternative. See also {@link NumericTableAccessor}.
	 */
	private final ThreadLocal<WeakReference<Pair<NumericReader[], ObjectReader<Instant>[]>>> readerReference =
			new ThreadLocal<>();

	/** all non-date-time columns in the order in which they appear in the table */
	private final List<Column> numericReadableColumns;

	/** all date-time columns in the order in which they appear in the table */
	private final List<Column> dateTimeColumns;

	/**
	 * map from original column index to the new index in a table when first taking the {@link #numericReadableColumns}
	 * and then the {@link #dateTimeColumns}
	 */
	private final int[] twist;

	/**
	 * Creates a new accessor for a belt table with date-time columns.
	 *
	 * @param table
	 * 		the table to wrap
	 * @param attributes
	 * 		the attributes matching the table, can contain {@code null} for unused columns
	 * @param numberOfDateTime
	 * 		the number of date-time columns
	 * @param unusedAttributes
	 * 		the number of {@code null}s in the attributes
	 */
	MixedTableAccessor(Table table, List<Attribute> attributes, int numberOfDateTime, int unusedAttributes) {
		super(table, attributes, unusedAttributes);
		twist = new int[table.width()];
		numericReadableColumns = new ArrayList<>();
		dateTimeColumns = new ArrayList<>();
		int normalCounter = 0;
		int dateTimeCounter = attributes.size() - numberOfDateTime;
		for (int i = 0; i < table.width(); i++) {
			Column column = table.column(i);
			// take the advanced columns a fake numeric-readable columns -> will get a {@code null} column reader
			if (column.type().hasCapability(Column.Capability.NUMERIC_READABLE) || !BeltConverter.STANDARD_TYPES.contains(column.type().id())) {
				numericReadableColumns.add(column);
				twist[i] = normalCounter++;
			} else {
				dateTimeColumns.add(column);
				twist[i] = dateTimeCounter++;
			}
		}
	}

	@Override
	Object getReaders() {
		NumericReader[] readers = new NumericReader[numericReadableColumns.size()];
		for (int i = 0; i < readers.length; i++) {
			Column column = numericReadableColumns.get(i);
			// ignore the advanced columns, will always return {@code 0} for them, see {@link AbstractTableAccessor#getNumericValue}
			if (BeltConverter.STANDARD_TYPES.contains(column.type().id())) {
				// use a small numeric reader instead of a normal one to lower the memory consumption by multiple readers
				readers[i] = SmallReaders.smallNumericReader(column);
			}
		}
		@SuppressWarnings("unchecked")
		ObjectReader<Instant>[] dateReaders = new ObjectReader[dateTimeColumns.size()];
		for (int i = 0; i < dateReaders.length; i++) {
			// use a small numeric reader instead of a normal one to lower the memory consumption by multiple readers
			dateReaders[i] = SmallReaders.smallObjectReader(dateTimeColumns.get(i), Instant.class);
		}
		return new Pair<>(readers, dateReaders);
	}

	@Override
	Object getUnbufferedReaders() {
		// if {@link ConvertOnWriteExampleTable#getDataRow} were only called for single rows, the following use of
		// threadlocal would not be necessary. But if {@link ConvertOnWriteExampleTable#getDataRow} is used to read a
		// whole table, this caching improves the performance
		WeakReference<Pair<NumericReader[], ObjectReader<Instant>[]>> pairWeakReference = readerReference.get();
		if (pairWeakReference != null) {
			Pair<NumericReader[], ObjectReader<Instant>[]> readerPair = pairWeakReference.get();
			if (readerPair != null) {
				return readerPair;
			}
		}
		//no cached readers found, create new ones and cache them
		NumericReader[] readers = new NumericReader[numericReadableColumns.size()];
		for (int i = 0; i < readers.length; i++) {
			Column column = numericReadableColumns.get(i);
			if (BeltConverter.STANDARD_TYPES.contains(column.type().id())) {
				readers[i] = SmallReaders.unbufferedNumericReader(column);
			}
		}
		@SuppressWarnings("unchecked")
		ObjectReader<Instant>[] dateReaders = new ObjectReader[dateTimeColumns.size()];
		for (int i = 0; i < dateReaders.length; i++) {
			dateReaders[i] = SmallReaders.unbufferedObjectReader(dateTimeColumns.get(i), Instant.class);
		}
		Pair<NumericReader[], ObjectReader<Instant>[]> readerPair = new Pair<>(readers, dateReaders);
		readerReference.set(new WeakReference<>(readerPair));
		return readerPair;
	}


	@Override
	double get(int row, int columnIndex, Object beltReader) {
		int indexInTwisted = twist[columnIndex];
		if (indexInTwisted < numericReadableColumns.size()) {
			@SuppressWarnings("unchecked")
			NumericReader[] firstReaders = ((Pair<NumericReader[], ObjectReader<Instant>[]>) beltReader).getFirst();
			return getNumericValue(row, columnIndex, firstReaders[indexInTwisted]);
		} else {
			@SuppressWarnings("unchecked")
			ObjectReader<Instant>[] secondReaders =
					((Pair<NumericReader[], ObjectReader<Instant>[]>) beltReader).getSecond();
			return getDateTime(row, indexInTwisted, secondReaders);
		}
	}

	@Override
	public AbstractTableAccessor columnCleanupClone(Attributes attributes) {
		Triple<Table, List<Attribute>, Integer> cleaned = columnCleanup(attributes);
		Table newTable = cleaned.getFirst();
		// need to count remaining date-time columns to use constructor
		int dateTimeCount = 0;
		for (Column column : newTable.getColumns()) {
			if (column.type().id() == Column.TypeId.DATE_TIME) {
				dateTimeCount++;
			}
		}
		return new MixedTableAccessor(newTable, cleaned.getSecond(), dateTimeCount, cleaned.getThird());
	}

	/**
	 * Get the date-time value at the given position by extracting the epoch millis.
	 */
	private double getDateTime(int rowIndex, int twistedColumnIndex, ObjectReader<Instant>[] readers) {
		//calculate index in object reader array
		int readerIndex = twistedColumnIndex - numericReadableColumns.size();
		ObjectReader<Instant> reader = readers[readerIndex];
		// set the position only if not already at the right position
		if (reader.position() != rowIndex - 1) {
			reader.setPosition(rowIndex - 1);
		}
		Instant instant = reader.read();
		return instant == null ? Double.NaN : instant.toEpochMilli();
	}
}