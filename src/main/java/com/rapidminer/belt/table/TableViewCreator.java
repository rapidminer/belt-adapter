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

import java.util.Arrays;
import java.util.Objects;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.example.ExampleSet;


/**
 * Creates a view of a {@link Table} that can be used for visualization purposes and reading as an {@link ExampleSet}.
 *
 * Please note that this class is not part of any public API and might be modified or removed in future releases without
 * prior warning.
 *
 * @author Gisa Meier
 */
public enum TableViewCreator{

	INSTANCE;

	/**
	 * Wraps the {@link Table} into an {@link ExampleSet} in order to visualize it.
	 *
	 * @param table
	 * 		the table
	 * @return a view example set
	 * @throws NullPointerException
	 * 		if table is {@code null}
	 * @throws BeltConverter.ConversionException
	 * 		if the table cannot be converted because it contains custom columns
	 */
	public ExampleSet createView(Table table) {
		Objects.requireNonNull(table, "table must not be null");

		table = removeDictionaryGaps(table);

		for (int i = 0; i < table.width(); i++) {
			if (table.column(i).type().id() == Column.TypeId.DATE_TIME) {
				return new DatetimeTableWrapper(table);
			}
		}
		return new DoubleTableWrapper(table);
	}

	/**
	 * Creates a new table where custom columns are replaced with nominal columns that are constant one error value.
	 *
	 * @param table
	 * 		the table to adjust
	 * @return a table without any custom columns
	 */
	public Table replacedCustomsWithError(Table table) {
		Column[] columns = table.getColumns();
		Column[] newColumns = Arrays.copyOf(columns, columns.length);
		for (int i = 0; i < columns.length; i++) {
			Column oldColumn = columns[i];
			if (oldColumn.type().id() == Column.TypeId.CUSTOM) {
				Column newColumn = ColumnAccessor.get().newSingleValueCategoricalColumn(ColumnTypes.NOMINAL, "Error:" +
						" Cannot display custom column of type " + oldColumn.type().customTypeID(), oldColumn.size());
				newColumns[i] = newColumn;
			}
		}
		return new Table(newColumns, table.labelArray(), table.getMetaData());
	}


	/**
	 * Replaces categorical columns with gap containing dictionaries with remapped ones.
	 */
	private Table removeDictionaryGaps(Table table) {
		Column[] newColumns = null;
		int index = 0;
		for (Column column : table.getColumns()) {
			if (column.type().id() == Column.TypeId.NOMINAL) {
				Dictionary<String> dict = column.getDictionary(String.class);
				if (dict.size() != dict.maximalIndex()) {
					if (newColumns == null) {
						newColumns = Arrays.copyOf(table.getColumns(), table.width());
					}
					newColumns[index] = Columns.compactDictionary(column);
				}
			}
			index++;
		}
		if (newColumns == null) {
			return table;
		} else {
			return new Table(newColumns, table.labelArray(), table.getMetaData());
		}
	}

}