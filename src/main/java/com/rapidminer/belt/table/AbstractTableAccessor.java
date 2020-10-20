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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.error.AttributeNotFoundError;
import com.rapidminer.tools.container.Triple;


/**
 * Common code for {@link MixedTableAccessor} and {@link NumericTableAccessor} that allows to access the belt {@link
 * Table} together with a list of attributes as needed by the {@link ConvertOnWriteExampleTable}. Consists mainly of the
 * attribute handling copied from {@link com.rapidminer.example.table.AbstractExampleTable}. Additionally, there are
 * methods to create a reader object and access a data point given a reader object.
 *
 * @author Gisa Meier
 * @since 0.7
 */
abstract class AbstractTableAccessor {

	private static final String EMPTY_STRING = "";

	protected final Table table;
	private final List<Attribute> attributes;
	private final int unusedAttributes;

	/**
	 * Creates a new accessor for a belt table.
	 *
	 * @param table
	 * 		the table to wrap
	 * @param attributes
	 * 		the attributes matching the table, can contain {@code null} for unused columns
	 * @param unusedAttributes
	 * 		the number of {@code null}s in the attributes
	 */
	AbstractTableAccessor(Table table, List<Attribute> attributes, int unusedAttributes) {
		this.attributes = attributes;
		this.table = table;
		this.unusedAttributes = unusedAttributes;
	}

	/**
	 * Get readers for the case that the whole table is going to be read.
	 *
	 * @return readers to use in an iterator over all rows
	 */
	abstract Object getReaders();

	/**
	 * Get readers for that one row is read without an iterator over all rows.
	 *
	 * @return readers to use for reading a single row
	 */
	abstract Object getUnbufferedReaders();

	/**
	 * Reads the value at (rowIndex, columnIndex) using the reader object. The reader object is a parameter here so
	 * that
	 * it can be cached.
	 *
	 * @param rowIndex
	 * 		the index of the row to read
	 * @param columnIndex
	 * 		the index of the column to read
	 * @param readerObject
	 * 		the reader object to use for reading
	 * @return the value at the specified position
	 */
	abstract double get(int rowIndex, int columnIndex, Object readerObject);


	// The following 3 methods are copied from {@link AbstractExampleTable}

	/**
	 * @return the attributes as a new array
	 */
	Attribute[] getAttributes() {
		Attribute[] attribute = new Attribute[attributes.size()];
		attributes.toArray(attribute);
		return attribute;
	}

	/**
	 * Returns the attribute of the column number {@code i}.
	 *
	 * @param i
	 * 		the column index
	 * @return the attribute with the given index
	 */
	Attribute getAttribute(int i) {
		return attributes.get(i);
	}

	/**
	 * Returns the attribute with the given name.
	 */
	Attribute findAttribute(String name) throws OperatorException {
		if (name == null) {
			return null;
		}
		for (Attribute att : attributes) {
			if (att != null && att.getName().equals(name)) {
				return att;
			}
		}
		throw new AttributeNotFoundError(null, null, name);
	}

	/**
	 * Gets the numeric value given a row and column index and a numeric reader. Shared code used in {@link
	 * NumericTableAccessor} and {@link MixedTableAccessor}.
	 *
	 * @param rowIndex
	 * 		the row index
	 * @param columnIndex
	 * 		the column index
	 * @param reader
	 * 		the reader to use
	 * @return the value at the specified position
	 */
	protected double getNumericValue(int rowIndex, int columnIndex, NumericReader reader) {
		// always return {@code 0} for advanced columns
		if (reader == null) {
			return 0;
		}
		// set the position only if not already at the right position
		if (reader.position() != rowIndex - 1) {
			reader.setPosition(rowIndex - 1);
		}
		// need to subtract {@code 1} in case of nominal attributes because of the shifted mapping in belt
		Attribute attribute = getAttribute(columnIndex);
		if (attribute.isNominal()) {
			return reader.read() - 1;
		} else {
			return reader.read();
		}
	}

	/**
	 * @return the underlying {@link Table}
	 */
	Table getTable() {
		return table;
	}

	/**
	 * Creates a copy of the {@link AbstractTableAccessor} where the unused columns have been replaced by dummy columns
	 * with minimal memory consumption.
	 *
	 * @param attributes
	 * 		the used attributes
	 * @return an accessor with cleaned up columns
	 */
	abstract AbstractTableAccessor columnCleanupClone(Attributes attributes);

	/**
	 * Creates a copy of the underlying {@link Table} where the unused columns have been replaced by dummy columns with
	 * minimal memory consumption.
	 *
	 * @param attributes
	 * 		the used attributes
	 * @return a triple of a table with cleaned up columns, attributes adjusted accordingly and the number of unused attributes
	 */
	protected Triple<Table, List<Attribute>, Integer> columnCleanup(Attributes attributes) {
		String[] labels = table.labelArray();
		Column[] oldColumns = table.getColumns();
		Column[] columns = Arrays.copyOf(oldColumns, oldColumns.length);

		boolean[] usedIndices = new boolean[table.width()];
		for (Iterator<Attribute> allIterator = attributes.allAttributes(); allIterator.hasNext(); ) {
			Attribute attribute = allIterator.next();
			int tableIndex = attribute.getTableIndex();
			if (tableIndex < usedIndices.length) {
				usedIndices[tableIndex] = true;
			}
		}
		//column taking minimal memory
		CategoricalColumn emptySparseColumn =
				ColumnAccessor.get().newSingleValueCategoricalColumn(ColumnType.NOMINAL, EMPTY_STRING,
						table.height());
		//replace unused columns by those which take minimal memory
		int unused = 0;
		List<Attribute> newAttributes = new ArrayList<>(this.attributes);
		for (int i = 0; i < columns.length; i++) {
			if (!usedIndices[i]) {
				columns[i] = emptySparseColumn;
				newAttributes.set(i, null);
				unused++;
			}
		}
		return new Triple<>(new Table(columns, labels, table.getMetaData()), newAttributes, unused);
	}

	/**
	 * @return the number of cleaned up attributes that are not used anymore
	 */
	int getUnused(){
		return unusedAttributes;
	}
}