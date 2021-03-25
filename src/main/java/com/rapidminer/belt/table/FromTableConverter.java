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

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.SimpleAttributes;
import com.rapidminer.example.set.HeaderExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.BinominalMapping;
import com.rapidminer.example.table.ExampleTable;
import com.rapidminer.example.table.NominalMapping;
import com.rapidminer.example.table.internal.ColumnarExampleTable;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.tools.Ontology;


/**
 * Converts from belt {@link Table}s to {@link ExampleSet}s.
 *
 * @author Gisa Meier
 * @since 0.7
 */
enum FromTableConverter {

	;//No instance enum

	/**
	 * Message for when non-supported columns types are encountered
	 */
	private static final String MESSAGE_NON_SUPPORTED = "Type not supported for now";

	/**
	 * Extracts a {@link HeaderExampleSet} from a table. This is useful for creating a {@link
	 * com.rapidminer.example.set.RemappedExampleSet} or specifying training header of a {@link
	 * com.rapidminer.operator.Model}.
	 *
	 * @param table
	 * 		the table to extract from
	 * @return a {@link HeaderExampleSet} where the nominal mappings of the attributes are immutable
	 * @throws BeltConverter.ConversionException
	 * 		if the table cannot be converted because it contains non-standard columns
	 */
	static HeaderExampleSet convertHeader(Table table) {
		Attributes attributes = new SimpleAttributes();
		List<String> labels = table.labels();
		int i = 0;
		for (String label : labels) {
			Column column = table.column(i);
			Attribute attribute = AttributeFactory.createAttribute(label, BeltConverter.getValueType(table, label, i));
			attribute.setTableIndex(i);
			attributes.add(new AttributeRole(attribute));
			if (attribute.isNominal()) {
				List<String> mapping = ColumnAccessor.get().getDictionaryList(column.getDictionary());
				attribute.setMapping(new NominalMappingAdapter(mapping,
						attribute.getValueType() == Ontology.BINOMINAL));
			}
			i++;
		}
		BeltConverter.convertRoles(table, attributes);
		HeaderExampleSet set = new HeaderExampleSet(attributes);
		BeltConverter.storeBeltMetaDataInExampleSetUserData(table, set);
		return set;
	}

	/**
	 * Converts a belt {@link IOTable} into an {@link ExampleSet}.
	 *
	 * @param tableObject
	 * 		the table object to convert
	 * @param context
	 * 		the context to use for parallel execution
	 * @return a new example set containing the values of the table
	 * @throws IllegalArgumentException
	 * 		if table or context is null
	 * @throws BeltConverter.ConversionException
	 * 		if the table cannot be converted because it contains non-standard columns
	 */
	static ExampleSet convert(IOTable tableObject, ConcurrencyContext context) {
		if (tableObject == null) {
			throw new IllegalArgumentException("Table object must not be null");
		}
		if (context == null) {
			throw new IllegalArgumentException("Context must not be null");
		}

		Table table = tableObject.getTable();
		List<Attribute> attributes = new ArrayList<>();
		List<String> labels = table.labels();
		int i = 0;
		for (String label : labels) {
			int valueType = BeltConverter.getValueType(table, label, i);
			attributes.add(AttributeFactory.createAttribute(label, valueType));
			i++;
		}

		ExampleSet set = ExampleSets.from(attributes).withBlankSize(table.height()).build();
		BeltConverter.storeBeltMetaDataInExampleSetUserData(table, set);
		ExampleTable exampleTable = set.getExampleTable();
		if (exampleTable instanceof ColumnarExampleTable) {
			ColumnarExampleTable columnTable = (ColumnarExampleTable) exampleTable;
			convertParallel(table, attributes, columnTable, context);
		} else {
			convertSequentially(table, set);
		}

		BeltConverter.convertRoles(table, set.getAttributes());
		//adjust attribute order so that it is kept instead of adding special attributes at the end
		adjustAttributes((Attributes)set.getAttributes().clone(), attributes, set);
		set.getAnnotations().addAll(tableObject.getAnnotations());
		set.setSource(tableObject.getSource());
		return set;
	}


	/**
	 * in order to keep the order of the attributes and not have specials at the end we add them again in the order of
	 * the attributeList.
	 */
	static void adjustAttributes(Attributes attributes, List<Attribute> attributeList, ExampleSet set) {
		Attributes orderedAttributes = set.getAttributes();
		orderedAttributes.clearRegular();
		orderedAttributes.clearSpecial();
		for (Attribute attribute : attributeList) {
			AttributeRole role = attributes.getRole(attribute);
			if (!role.isSpecial()) {
				orderedAttributes.addRegular(attribute);
			} else {
				AttributeRole attributeRole = new AttributeRole(attribute);
				attributeRole.setSpecial(role.getSpecialName());
				orderedAttributes.add(attributeRole);
			}
		}
	}

	/**
	 * Converts a table object into an example set sequentially in case no operator is known. If possible, {@link
	 * #convert(IOTable, ConcurrencyContext)} should be preferred.
	 *
	 * @param tableObject
	 * 		the table object to convert
	 * @return the example set
	 * @throws BeltConverter.ConversionException
	 * 		if the table cannot be converted because it contains non-standard columns
	 */
	static ExampleSet convertSequentially(IOTable tableObject) {
		if (tableObject == null) {
			throw new IllegalArgumentException("Table object must not be null");
		}

		Table table = tableObject.getTable();
		List<Attribute> attributes = new ArrayList<>();
		List<String> labels = table.labels();
		int i = 0;
		for (String label : labels) {
			int valueType = BeltConverter.getValueType(table, label, i);
			attributes.add(AttributeFactory.createAttribute(label, valueType));
			i++;
		}

		ExampleSet set = ExampleSets.from(attributes).withBlankSize(table.height()).build();
		BeltConverter.storeBeltMetaDataInExampleSetUserData(table, set);
		convertSequentially(table, set);
		BeltConverter.convertRoles(table, set.getAttributes());
		set.getAnnotations().addAll(tableObject.getAnnotations());
		set.setSource(tableObject.getSource());
		return set;
	}

	/**
	 * Converts the table for the {@link ConvertOnWriteExampleTable}.
	 *
	 * @param table
	 * 		the underlying belt {@link Table}
	 * @param attributes
	 * 		the attributes for the table
	 * @return a new {@link ColumnarExampleTable}
	 */
	static ColumnarExampleTable convert(Table table, Attribute[] attributes) {
		List<Attribute> attributeList = Arrays.asList(attributes);
		//replace nulls by dummy attributes
		List<Attribute> dummyAttributes = new ArrayList<>();
		for (int i = 0; i < attributeList.size(); i++) {
			if (attributeList.get(i) == null) {
				Attribute dummy = AttributeFactory.createAttribute("", Ontology.NUMERICAL);
				dummyAttributes.add(dummy);
				attributeList.set(i, dummy);
			}
		}

		ColumnarExampleTable columnarExampleTable = new ColumnarExampleTable(attributeList);
		columnarExampleTable.addBlankRows(table.height());
		columnarExampleTable.setExpectedSize(table.height());

		for (Attribute dummyAttribute : dummyAttributes) {
			columnarExampleTable.removeAttribute(dummyAttribute);
		}
		ExampleSet exampleSet = columnarExampleTable.createExampleSet();
		// replace the same way as it is displayed in the view
		table = TableViewCreator.INSTANCE.replaceAdvancedWithErrorMessage(table, x -> TableViewCreator.CANNOT_DISPLAY_MESSAGE);
		convertSequentially(table, exampleSet);
		columnarExampleTable.complete();

		return columnarExampleTable;
	}

	/**
	 * While studio does not explicitly forbid {@code null} values in dictionaries, some places assume that there are
	 * none, so we adjust all belt dictionaries with this problem.
	 *
	 * @param column
	 * 		a nominal column
	 */
	private static Column removeGapsFromDictionary(Column column) {
		return Columns.compactDictionary(column);
	}

	/**
	 * Copies the data from the table into the set sequentially.
	 */
	private static void convertSequentially(Table table, ExampleSet set) {
		for (Attribute attribute : set.getAttributes()) {
			Column column = table.column(attribute.getTableIndex());
			switch (attribute.getValueType()) {
				case Ontology.STRING:
				case Ontology.FILE_PATH:
				case Ontology.NOMINAL:
				case Ontology.POLYNOMINAL:
					copyToNominal(set, attribute, column);
					break;
				case Ontology.BINOMINAL:
					copyToBinominal(set, attribute, column);
					break;
				case Ontology.NUMERICAL:
				case Ontology.REAL:
				case Ontology.INTEGER:
					NumericReader reader = Readers.numericReader(column, column.size());
					for (Example example : set) {
						example.setValue(attribute, reader.read());
					}
					break;
				case Ontology.TIME:
					if (column.type().id() == Column.TypeId.TIME) {
						copyToTime(set, attribute, column);
					} else {
						// date-time can be converted to time for legacy reasons
						copyToDateTime(set, attribute, column);
					}
					break;
				case Ontology.DATE_TIME:
				case Ontology.DATE:
					copyToDateTime(set, attribute, column);
					break;
				default:
					throw new UnsupportedOperationException(MESSAGE_NON_SUPPORTED);
			}
		}
	}

	private static void copyToDateTime(ExampleSet set, Attribute attribute, Column column) {
		ObjectReader<Instant> reader =
				Readers.objectReader(column, Instant.class);
		for (Example example : set) {
			Instant read = reader.read();
			if (read == null) {
				example.setValue(attribute, Double.NaN);
			} else {
				example.setValue(attribute, BeltConverter.toEpochMilli(read));
			}
		}
	}

	private static void copyToTime(ExampleSet set, Attribute attribute, Column column) {
		ObjectReader<LocalTime> reader =
				Readers.objectReader(column, LocalTime.class);
		for (Example example : set) {
			LocalTime read = reader.read();
			if (read == null) {
				example.setValue(attribute, Double.NaN);
			} else {
				// add the negative time zone offset since the time zone offset gets added
				// for the legacy time in studio view and operators
				example.setValue(attribute, BeltConverter.nanoOfDayToLegacyTime(read.toNanoOfDay()));
			}
		}
	}

	private static void copyToNominal(ExampleSet set, Attribute attribute, Column column) {
		column = removeGapsFromDictionary(column);

		copyNewToOldMapping(attribute, column);
		CategoricalReader reader = Readers.categoricalReader(column);
		for (Example example : set) {
			int read = reader.read();
			if (read == CategoricalReader.MISSING_CATEGORY) {
				example.setValue(attribute, Double.NaN);
			} else {
				example.setValue(attribute, read - 1d);
			}
		}
	}

	private static void copyToBinominal(ExampleSet set, Attribute attribute, Column column) {
		column = removeGapsFromDictionary(column);

		Dictionary dictionary = column.getDictionary();
		List<String> mapping = ColumnAccessor.get().getDictionaryList(dictionary);
		if (dictionary.isBoolean()) {
			// check if last value is positive
			if (dictionary.getPositiveIndex() == 2 || !dictionary.hasPositive()) {
				copyNegativePositive(set, attribute, column, dictionary);
			} else {
				copyPositiveNegative(set, attribute, column, mapping);
			}
		} else {
			copyToNominal(set, attribute, column);
		}
	}

	/**
	 * Copy binominals from table to mapping in case the mapping contains first the positive, then the negative value.
	 */
	private static void copyPositiveNegative(ExampleSet set, Attribute attribute, Column column, List<String>
			mapping) {
		//the second mapped value is negative, we have to swap indices
		int positiveIndex = mapping.size() - 2;
		int negativeIndex = mapping.size() - 1;
		NominalMapping legacyMapping = attribute.getMapping();
		//the first mapped value is negative
		legacyMapping.mapString(mapping.get(negativeIndex));
		legacyMapping.mapString(mapping.get(positiveIndex));
		CategoricalReader reader = Readers.categoricalReader(column);
		for (Example example : set) {
			int read = reader.read();
			if (read == negativeIndex) {
				example.setValue(attribute, BinominalMapping.NEGATIVE_INDEX);
			} else if (read == positiveIndex) {
				example.setValue(attribute, BinominalMapping.POSITIVE_INDEX);
			} else {
				example.setValue(attribute, Double.NaN);
			}
		}
	}

	/**
	 * Copy binominals from table to mapping in case the mapping contains first the negative, then the positive value.
	 */
	private static void copyNegativePositive(ExampleSet set, Attribute attribute, Column column, Dictionary
			mapping) {
		NominalMapping legacyMapping = attribute.getMapping();
		//the first mapped value is negative, the order is kept
		for (Dictionary.Entry value : mapping) {
			legacyMapping.mapString(value.getValue());
		}
		CategoricalReader reader = Readers.categoricalReader(column);
		for (Example example : set) {
			int read = reader.read();
			if (read == CategoricalReader.MISSING_CATEGORY) {
				example.setValue(attribute, Double.NaN);
			} else {
				example.setValue(attribute, read - 1d);
			}
		}
	}


	/**
	 * Copies the given table into the given columnTable. Copies each of the given attributes in parallel using the
	 * given context.
	 */
	private static void convertParallel(Table table, List<Attribute> attributes,
										ColumnarExampleTable columnTable, ConcurrencyContext context) {
		List<Callable<Void>> copier = new ArrayList<>(table.width());
		int i = 0;
		for (Attribute attribute : attributes) {
			Column column = table.column(i++);
			switch (attribute.getValueType()) {
				case Ontology.STRING:
				case Ontology.FILE_PATH:
				case Ontology.NOMINAL:
				case Ontology.POLYNOMINAL:
					copier.add(() ->
							copyNominalColumnToRows(columnTable, attribute, column));
					break;
				case Ontology.BINOMINAL:
					copier.add(() ->
							copyBinominalColumnToRows(columnTable, attribute, column));
					break;
				case Ontology.NUMERICAL:
				case Ontology.REAL:
				case Ontology.INTEGER:
					copier.add(() -> {
						NumericReader reader =
								Readers.numericReader(column);
						for (int row = 0; row < columnTable.size(); row++) {
							columnTable.getDataRow(row).set(attribute, reader.read());
						}
						return null;
					});
					break;
				case Ontology.TIME:
					if (column.type().id() == Column.TypeId.TIME) {
						copier.add(() -> copyTimeColumnToRows(columnTable, attribute, column));
					} else {
						// date-time can be converted to time for legacy reasons
						copier.add(() -> copyDateTimeColumnToRows(columnTable, attribute, column));
					}
					break;
				case Ontology.DATE_TIME:
				case Ontology.DATE:
					copier.add(() -> copyDateTimeColumnToRows(columnTable, attribute, column));
					break;
				default:
					throw new UnsupportedOperationException(MESSAGE_NON_SUPPORTED);
			}
		}
		try {
			context.call(copier);
		} catch (ExecutionException e) {
			ToTableConverter.handleExecutionException(e);
		}
	}

	private static Void copyDateTimeColumnToRows(ColumnarExampleTable columnTable, Attribute attribute, Column
			column) {
		ObjectReader<Instant> reader = Readers.objectReader(column, Instant.class);
		for (int row = 0; row < columnTable.size(); row++) {
			Instant read = reader.read();
			if (read == null) {
				columnTable.getDataRow(row).set(attribute, Double.NaN);
			} else {
				columnTable.getDataRow(row).set(attribute, BeltConverter.toEpochMilli(read));
			}
		}
		return null;
	}

	private static Void copyTimeColumnToRows(ColumnarExampleTable columnTable, Attribute attribute, Column
			column) {
		ObjectReader<LocalTime> reader = Readers.objectReader(column, LocalTime.class);
		for (int row = 0; row < columnTable.size(); row++) {
			LocalTime read = reader.read();
			if (read == null) {
				columnTable.getDataRow(row).set(attribute, Double.NaN);
			} else {
				// add the negative time zone offset since the time zone offset gets added
				// for the legacy time in studio view and operators
				columnTable.getDataRow(row).set(attribute, BeltConverter.nanoOfDayToLegacyTime(read.toNanoOfDay()));
			}
		}
		return null;
	}

	private static Void copyBinominalColumnToRows(ColumnarExampleTable columnTable, Attribute attribute,
												  Column column) {
		column = removeGapsFromDictionary(column);

		Dictionary dictionary = column.getDictionary();
		if (dictionary.isBoolean()) {
			List<String> mapping = ColumnAccessor.get().getDictionaryList(dictionary);
			// check if last value is positive
			if (dictionary.getPositiveIndex() == 2 || !dictionary.hasPositive()) {
				copyNegativePositiveToRows(columnTable, attribute, column, dictionary);
			} else {
				copyPositiveNegativeToRows(columnTable, attribute, column, mapping);

			}
		} else {
			copyNominalColumnToRows(columnTable, attribute, column);
		}
		return null;
	}

	/**
	 * Copy binominals from table to mapping in case the mapping contains first the positive, then the negative value.
	 */
	private static void copyPositiveNegativeToRows(ColumnarExampleTable columnTable, Attribute attribute, Column
			column,
												   List<String> mapping) {
		//the second mapped value is negative, we have to swap indices
		int positiveIndex = mapping.size() - 2;
		int negativeIndex = mapping.size() - 1;

		NominalMapping legacyMapping = attribute.getMapping();
		//the first mapped value is negative
		legacyMapping.mapString(mapping.get(negativeIndex));
		legacyMapping.mapString(mapping.get(positiveIndex));
		CategoricalReader reader = Readers.categoricalReader(column);
		for (int row = 0; row < columnTable.size(); row++) {
			int read = reader.read();
			if (read == negativeIndex) {
				columnTable.getDataRow(row).set(attribute, BinominalMapping.NEGATIVE_INDEX);
			} else if (read == positiveIndex) {
				columnTable.getDataRow(row).set(attribute, BinominalMapping.POSITIVE_INDEX);
			} else {
				columnTable.getDataRow(row).set(attribute, Double.NaN);
			}
		}
	}

	/**
	 * Copy binominals from table to mapping in case the mapping contains first the negative, then the positive value.
	 */
	private static void copyNegativePositiveToRows(ColumnarExampleTable columnTable, Attribute attribute,
												   Column column, Dictionary mapping) {
		//the first mapped value is negative, the order is kept
		NominalMapping legacyMapping = attribute.getMapping();
		for (Dictionary.Entry value : mapping) {
			legacyMapping.mapString(value.getValue());
		}
		CategoricalReader reader = Readers.categoricalReader(column);
		for (int row = 0; row < columnTable.size(); row++) {
			int read = reader.read();
			if (read == CategoricalReader.MISSING_CATEGORY) {
				columnTable.getDataRow(row).set(attribute, Double.NaN);
			} else {
				columnTable.getDataRow(row).set(attribute, read - 1d);
			}
		}
	}

	private static Void copyNominalColumnToRows(ColumnarExampleTable columnTable, Attribute attribute, Column column) {
		column = removeGapsFromDictionary(column);

		copyNewToOldMapping(attribute, column);
		CategoricalReader reader = Readers.categoricalReader(column);
		for (int row = 0; row < columnTable.size(); row++) {
			int read = reader.read();
			if (read == CategoricalReader.MISSING_CATEGORY) {
				columnTable.getDataRow(row).set(attribute, Double.NaN);
			} else {
				columnTable.getDataRow(row).set(attribute, read - 1d);
			}
		}
		return null;
	}

	private static void copyNewToOldMapping(Attribute attribute, Column column) {
		List<String> mapping = ColumnAccessor.get().getDictionaryList(column.getDictionary());
		NominalMapping legacyMapping = attribute.getMapping();
		for (int j = 1; j < mapping.size(); j++) {
			legacyMapping.mapString(mapping.get(j));
		}
	}

}
