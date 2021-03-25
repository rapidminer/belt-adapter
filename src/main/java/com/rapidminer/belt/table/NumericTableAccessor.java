/**
 * Copyright (C) 2001-2021 by RapidMiner and the contributors
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
import java.util.List;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.SmallReaders;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.tools.container.Triple;


/**
 * {@link AbstractTableAccessor} for the case that there are no date-time columns, i.e. everything can be read
 * numerically. While reading a whole table numerically would be normally an use case for a {@link
 * com.rapidminer.belt.reader.NumericRowReader}, benchmarks have shown that using a row reader has horrible performance
 * when the table is read column-wise. Therefore, we use an array of column readers which make column-wise reading much
 * faster and row-wise reading only slightly slower.
 *
 * @author Gisa Meier
 * @since 0.7
 */
class NumericTableAccessor extends AbstractTableAccessor {

	/**
	 * {@link ThreadLocal} with a weak reference that holds the array of numeric readers. The {@link WeakReference} is
	 * needed since otherwise the readers and thus all columns are kept as long as the thread is alive even if this
	 * {@link NumericTableAccessor} is already gone. With the weak reference, we might have to recreate the readers in
	 * case of garbage collection but this is better than the alternative. See also
	 * https://dzone.com/articles/painless-introduction-javas-threadlocal-storage
	 */
	private final ThreadLocal<WeakReference<NumericReader[]>> readersReference = new ThreadLocal<>();

	/**
	 * Creates a new accessor for a belt table without date-time columns.
	 *
	 * @param table
	 * 		the table to wrap
	 * @param attributes
	 * 		the attributes matching the table, can contain {@code null} for unused columns
	 * @param unusedAttributes
	 * 		the number of {@code null}s in the attributes
	 */
	NumericTableAccessor(Table table, List<Attribute> attributes, int unusedAttributes) {
		super(table, attributes, unusedAttributes);
	}

	@Override
	Object getReaders() {
		NumericReader[] readers = new NumericReader[table.width()];
		for (int i = 0; i < readers.length; i++) {
			Column column = table.column(i);
			// ignore the advanced columns, will always return {@code 0} for them, see {@link AbstractTableAccessor#getNumericValue}
			if (BeltConverter.STANDARD_TYPES.contains(column.type().id())) {
				// use a small numeric reader instead of a normal one to lower the memory consumption by multiple readers
				readers[i] = SmallReaders.smallNumericReader(column);
			}
		}
		return readers;
	}

	@Override
	Object getUnbufferedReaders() {
		// if {@link ConvertOnWriteExampleTable#getDataRow} were only called for single rows, the following use of
		// threadlocal would not be necessary. But if {@link ConvertOnWriteExampleTable#getDataRow} is used to read a
		// whole table, this caching improves the performance
		WeakReference<NumericReader[]> numericReadersWeakReference = readersReference.get();
		if (numericReadersWeakReference != null) {
			NumericReader[] numericReaders = numericReadersWeakReference.get();
			if (numericReaders != null) {
				return numericReaders;
			}
		}
		//no cached readers found, create new ones and cache them
		NumericReader[] readers = new NumericReader[table.width()];
		for (int i = 0; i < readers.length; i++) {
			Column column = table.column(i);
			if (BeltConverter.STANDARD_TYPES.contains(column.type().id())) {
				readers[i] = SmallReaders.unbufferedNumericReader(column);
			}
		}
		readersReference.set(new WeakReference<>(readers));
		return readers;
	}


	@Override
	double get(int rowIndex, int columnIndex, Object readerObject) {
		NumericReader[] readers = (NumericReader[]) readerObject;
		return getNumericValue(rowIndex, columnIndex, readers[columnIndex]);
	}

	@Override
	public AbstractTableAccessor columnCleanupClone(Attributes attributes) {
		Triple<Table, List<Attribute>, Integer> cleaned = columnCleanup(attributes);
		return new NumericTableAccessor(cleaned.getFirst(), cleaned.getSecond(), cleaned.getThird());
	}

}