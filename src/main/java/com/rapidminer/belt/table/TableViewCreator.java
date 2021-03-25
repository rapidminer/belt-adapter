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

import static com.rapidminer.belt.table.BeltConverter.ConversionException;
import static com.rapidminer.belt.table.BeltConverter.STANDARD_TYPES;
import static com.rapidminer.belt.table.BeltConverter.convertRoles;
import static com.rapidminer.belt.table.BeltConverter.getValueType;
import static com.rapidminer.belt.table.BeltConverter.storeBeltMetaDataInExampleSetUserData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.SimpleAttributes;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.BinominalAttribute;
import com.rapidminer.example.table.BinominalMapping;
import com.rapidminer.example.table.NominalMapping;
import com.rapidminer.tools.Ontology;


/**
 * Creates a view of a {@link Table} that can be used for visualization purposes and reading as an {@link ExampleSet}
 * or creates a view that does a conversion on the fly if necessary.
 *
 * Please note that this class is not part of any public API and might be modified or removed in future releases without
 * prior warning.
 *
 * @author Gisa Meier
 */
public enum TableViewCreator{

	INSTANCE;

	/**
	 * Message for constant replacement of advanced columns in {@link ConvertOnWriteExampleTable}
	 */
	static final String CANNOT_DISPLAY_MESSAGE = "Cannot display advanced column";

	/**
	 * Constant mapping with only an error message entry
	 */
	private static final NominalMapping CANNOT_DISPLAY = new ShiftedNominalMappingAdapter(Arrays.asList(null, CANNOT_DISPLAY_MESSAGE));

	/**
	 * Wraps the {@link Table} into an {@link ExampleSet} in order to visualize it.
	 *
	 * @param table
	 * 		the table
	 * @return a view example set
	 * @throws NullPointerException
	 * 		if table is {@code null}
	 * @throws ConversionException
	 * 		if the table cannot be converted because it contains advanced columns
	 */
	public ExampleSet createView(Table table) {
		Objects.requireNonNull(table, "table must not be null");

		table = adjustDictionaries(table);

		for (int i = 0; i < table.width(); i++) {
			Column.TypeId id = table.column(i).type().id();
			if (id == Column.TypeId.DATE_TIME || id == Column.TypeId.TIME) {
				return new DatetimeTableWrapper(table);
			}
		}
		return new DoubleTableWrapper(table);
	}

	/**
	 * Wraps the {@link Table} of the {@link IOTable} into an {@link ExampleSet} so that adding additional attributes
	 * works without conversion.
	 *
	 * @param ioTable
	 * 		the table to view as an {@link ExampleSet}
	 * @param throwOnAdvanced
	 * 		whether to throw an exception in case of advanced columns. If this is {@code false} the advanced column is
	 * 		viewed as a nominal column with a constant error message and it is recovered on the conversion back to {@link
	 *        IOTable}
	 * @return a view of the ioTable that only does a conversion on a write operation into existing table data
	 * @throws ConversionException
	 * 		if the table contains advanced columns and thrownOnAdvanced is {@code true}
	 * @since 0.7
	 */
	public ExampleSet convertOnWriteView(IOTable ioTable, boolean throwOnAdvanced) {
		Table table = ioTable.getTable();
		table = TableViewCreator.INSTANCE.adjustDictionaries(table);
		Attributes attributes = new SimpleAttributes();
		List<Attribute> attributeList = new ArrayList<>();
		List<String> labels = table.labels();
		int numberOfDatetime = 0;
		int i = 0;
		for (String label : labels) {
			Column column = table.column(i);
			Attribute attribute;
			if (STANDARD_TYPES.contains(column.type().id())) {
				attribute = AttributeFactory.createAttribute(label,
						getValueType(table, label, i));
				if (attribute.isNominal()) {
					setMapping(column, attribute);
				} else if (column.type().id() == Column.TypeId.DATE_TIME) {
					numberOfDatetime++;
				}
			} else {
				if (throwOnAdvanced) {
					throw new ConversionException(label, column.type());
				} else {
					attribute = AttributeFactory.createAttribute(label, Ontology.NOMINAL);
					attribute.setMapping(CANNOT_DISPLAY);
				}
			}
			attribute.setTableIndex(i);
			attributes.add(new AttributeRole(attribute));
			attributeList.add(attribute);

			i++;
		}
		convertRoles(table, attributes);
		ExampleSet set = new ConvertOnWriteExampleTable(table, attributeList, numberOfDatetime).createExampleSet();
		FromTableConverter.adjustAttributes(attributes, attributeList, set);
		set.getAnnotations().addAll(ioTable.getAnnotations());
		set.setSource(ioTable.getSource());
		storeBeltMetaDataInExampleSetUserData(table, set);
		return set;
	}


	/**
	 * Creates a new table where advanced columns are replaced with nominal columns that are constant one error value.
	 *
	 * @param table
	 * 		the table to adjust
	 * @return a table without any advanced columns
	 */
	public Table replacedAdvancedWithError(Table table) {
		return replaceAdvancedWithErrorMessage(table, oldColumn -> "Error:" +
				" Cannot display advanced column of " + oldColumn.type());
	}

	/**
	 * Compacts all nominal dictionaries with gaps.
	 *
	 * @param table
	 * 		the table with the dictionaries to compact
	 * @return a new table with all columns with compact dictionaries or the same table if that was	already the case
	 */
	public Table compactDictionaries(Table table) {
		Column[] newColumns = null;
		int index = 0;
		for (Column column : table.getColumns()) {
			if (column.type().id() == Column.TypeId.NOMINAL) {
				Dictionary dict = column.getDictionary();
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

	/**
	 * Creates a new table where advanced columns are replaced with nominal columns that are constant one error value.
	 *
	 * @param table
	 * 		the table to adjust
	 * @param errorMessage
	 * 		the error message to use
	 * @return a table without any advanced columns
	 */
	Table replaceAdvancedWithErrorMessage(Table table, Function<Column, String> errorMessage) {
		if (table.width() == 0) {
			return table;
		}
		Column[] columns = table.getColumns();
		Column[] newColumns = Arrays.copyOf(columns, columns.length);
		for (int i = 0; i < columns.length; i++) {
			Column oldColumn = columns[i];
			if (!STANDARD_TYPES.contains(oldColumn.type().id())) {
				Column newColumn = ColumnAccessor.get().newSingleValueCategoricalColumn(ColumnType.NOMINAL,
						errorMessage.apply(oldColumn), oldColumn.size());
				newColumns[i] = newColumn;
			}
		}
		return new Table(newColumns, table.labelArray(), table.getMetaData());
	}


	/**
	 * Replaces categorical columns with gap containing dictionaries with remapped ones and remapps columns with
	 * boolean dictionaries that have not the negative index as first index.
	 *
	 * Package private for tests.
	 */
	Table adjustDictionaries(Table table) {
		Column[] newColumns = null;
		int index = 0;
		for (Column column : table.getColumns()) {
			if (column.type().id() == Column.TypeId.NOMINAL) {
				Dictionary dict = column.getDictionary();
				if (dict.size() != dict.maximalIndex()) {
					if (newColumns == null) {
						newColumns = Arrays.copyOf(table.getColumns(), table.width());
					}
					newColumns[index] = Columns.compactDictionary(column);
					dict = newColumns[index].getDictionary();

				}
				if (dict.isBoolean() && dict.getNegativeIndex() != 1 && dict.size() > 0) {
					//binominal attributes need to have the first index as negative

					CategoricalColumn rightDictionaryColumn = getColumnWithAdjustedDictionary(dict);

					if (newColumns == null) {
						newColumns = Arrays.copyOf(table.getColumns(), table.width());
					}
					newColumns[index] = Columns.changeDictionary(column, rightDictionaryColumn);
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

	/**
	 *  Creates a column with a dictionary that has the negative value of the boolean dictionary first.
	 */
	private CategoricalColumn getColumnWithAdjustedDictionary(Dictionary dict) {
		//This a bit of a hack that uses implementation details of the categorical buffer
		NominalBuffer rightDictionaryBuffer = Buffers.nominalBuffer(2, 2);
		String negativeValue = dict.get(dict.getNegativeIndex());
		if (negativeValue == null) {
			negativeValue = "false";
		}
		rightDictionaryBuffer.set(0, negativeValue);
		String positiveValue = dict.get(dict.getPositiveIndex());
		rightDictionaryBuffer.set(1, positiveValue);
		return rightDictionaryBuffer.toBooleanColumn(positiveValue);
	}


	/**
	 * Converts the dictionary of the column to a nominal mapping and sets it for the attribute.
	 */
	private void setMapping(Column column, Attribute attribute) {
		List<String> mapping = ColumnAccessor.get().getDictionaryList(column.getDictionary());
		if (attribute instanceof BinominalAttribute) {
			BinominalMapping binMapping = new BinominalMapping();
			if (mapping.size() > 1) {
				binMapping.mapString(mapping.get(1));
			}
			if (mapping.size() > 2) {
				binMapping.mapString(mapping.get(2));
			}
			attribute.setMapping(binMapping);
		} else {
			attribute.setMapping(new ShiftedNominalMappingAdapter(mapping));
		}
	}



}