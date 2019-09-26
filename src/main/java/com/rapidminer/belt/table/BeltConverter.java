/**
 * Copyright (C) 2001-2019 by RapidMiner and the contributors
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.rapidminer.adaption.belt.ContextAdapter;
import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.CategoricalBuffer;
import com.rapidminer.belt.buffer.DateTimeBuffer;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnReference;
import com.rapidminer.belt.util.ColumnRole;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;
import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.SimpleAttributes;
import com.rapidminer.example.set.AbstractExampleSet;
import com.rapidminer.example.set.HeaderExampleSet;
import com.rapidminer.example.set.SimpleExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.BinominalAttribute;
import com.rapidminer.example.table.BinominalMapping;
import com.rapidminer.example.table.DateAttribute;
import com.rapidminer.example.table.ExampleTable;
import com.rapidminer.example.table.NominalMapping;
import com.rapidminer.example.table.NumericalAttribute;
import com.rapidminer.example.table.PolynominalAttribute;
import com.rapidminer.example.table.internal.ColumnarExampleTable;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.Ontology;


/**
 * Converts between {@link ExampleSet}s and belt {@link Table}s. For now supports only numeric attributes.
 *
 * Please note that this class is not part of any public API and might be modified or removed in future releases without
 * prior warning.
 *
 * @author Gisa Meier
 */
public final class BeltConverter {

	/**
	 * Message for when non-supported columns types are encountered
	 */
	private static final String MESSAGE_NON_SUPPORTED = "Type not supported for now";

	/**
	 * Set of primitive attribute types that are known to be thread safe for read accesses.
	 */
	private static final Set<Class<? extends Attribute>> SAFE_ATTRIBUTES = new HashSet<>(5);

	/**
	 * Number of milli-seconds in a second
	 */
	private static final long MILLISECONDS_PER_SECOND = 1_000;

	/**
	 * Number of nano-seconds in a milli-second
	 */
	private static final long NANOS_PER_MILLI_SECOND = 1_000_000;

	/**
	 * String into which {@link ColumnRole#METADATA} is converted
	 */
	private static final String META_DATA_NAME = "meta_data";

	/**
	 * Prefix of the role names of confidence attributes
	 */
	private static final String CONFIDENCE_PREFIX = Attributes.CONFIDENCE_NAME + "_";

	/**
	 * The length of the {@link #CONFIDENCE_PREFIX}
	 */
	private static final int CONFIDENCE_PREFIX_LENGHT = CONFIDENCE_PREFIX.length();

	static {
		SAFE_ATTRIBUTES.add(DateAttribute.class);
		SAFE_ATTRIBUTES.add(BinominalAttribute.class);
		SAFE_ATTRIBUTES.add(PolynominalAttribute.class);
		SAFE_ATTRIBUTES.add(NumericalAttribute.class);
	}

	// Suppress default constructor for noninstantiability
	private BeltConverter() {throw new AssertionError();}

	/**
	 * Creates a belt {@link IOTable} from the given {@link ExampleSet}. This is done in parallel if the
	 * exampleSet is threadsafe.
	 *
	 * @param exampleSet
	 *            the exampleSet to convert
	 * @param context
	 *            the concurrency context to use for the conversion
	 * @return a belt table
	 */
	public static IOTable convert(ExampleSet exampleSet, ConcurrencyContext context) {
		if (exampleSet == null) {
			throw new IllegalArgumentException("Example set must not be null");
		}
		if (context == null) {
			throw new IllegalArgumentException("Context must not be null");
		}

		// check example set implementation
		boolean threadSafeView = exampleSet instanceof AbstractExampleSet
				&& ((AbstractExampleSet) exampleSet).isThreadSafeView();
		boolean simpleView = exampleSet.getClass() == SimpleExampleSet.class;
		boolean threadSafe = threadSafeView;

		// check example table implementation
		if (threadSafe) {
			ExampleTable table = exampleSet.getExampleTable();
			threadSafe = table.getClass() == ColumnarExampleTable.class;
		}

		// check attribute implementation
		if (threadSafe) {
			Attributes attributes = exampleSet.getAttributes();
			threadSafe = attributes.getClass() == SimpleAttributes.class;
		}

		// check individual attributes and attribute transformations
		if (threadSafe) {
			Iterator<Attribute> attributes = exampleSet.getAttributes().allAttributes();
			while (threadSafe && attributes.hasNext()) {
				Attribute attribute = attributes.next();
				if (!SAFE_ATTRIBUTES.contains(attribute.getClass()) || attribute.getLastTransformation() != null) {
					threadSafe = false;
				}
			}
		}
		Table table;
		if (threadSafe) {
			// we can safely read from the input example using multiple threads
			if (simpleView) {
				// we can ignore the view and read directly from the underlying example table
				table = exampleTableConvert(exampleSet, context);
			} else {
				table = parallelConvert(exampleSet, context);
			}
		} else {
			table = sequentialConvert(exampleSet, context);
		}
		IOTable tableObject = new IOTable(table);
		tableObject.getAnnotations().addAll(exampleSet.getAnnotations());
		tableObject.setSource(exampleSet.getSource());
		return tableObject;
	}

	/**
	 * Extracts a {@link HeaderExampleSet} from a table. This is useful for creating a {@link
	 * com.rapidminer.example.set.RemappedExampleSet} or specifying training header of a {@link
	 * com.rapidminer.operator.Model}.
	 *
	 * @param table
	 * 		the table to extract from
	 * @return a {@link HeaderExampleSet} where the nominal mappings of the attributes are immutable
	 */
	public static HeaderExampleSet convertHeader(Table table) {
		Attributes attributes = new SimpleAttributes();
		List<String> labels = table.labels();
		int i = 0;
		for (String label : labels) {
			Column column = table.column(i);
			Attribute attribute = AttributeFactory.createAttribute(label, getValueType(table, label, i));
			attribute.setTableIndex(i);
			attributes.add(new AttributeRole(attribute));
			if (attribute.isNominal()) {
				List<String> mapping = ColumnAccessor.get().getDictionaryList(column.getDictionary(String.class));
				attribute.setMapping(new NominalMappingAdapter(mapping));
			}
			String role = convertRole(table, label);
			if (role != null) {
				attributes.setSpecialAttribute(attribute, role);
			}
			i++;
		}
		return new HeaderExampleSet(attributes);
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
	 */
	public static ExampleSet convert(IOTable tableObject, ConcurrencyContext context) {
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
			int valueType = getValueType(table, label, i);
			attributes.add(AttributeFactory.createAttribute(label, valueType));
			i++;
		}

		ExampleSet set = ExampleSets.from(attributes).withBlankSize(table.height()).build();
		ExampleTable exampleTable = set.getExampleTable();
		if (exampleTable instanceof ColumnarExampleTable) {
			ColumnarExampleTable columnTable = (ColumnarExampleTable) exampleTable;
			convertParallel(table, attributes, columnTable, context);
		} else {
			convertSequentially(table, set);
		}

		Attributes allAttributes = set.getAttributes();
		for (String label : labels) {
			String studioRole = convertRole(table, label);
			if (studioRole != null && checkUnique(allAttributes, studioRole)) {
				allAttributes.setSpecialAttribute(allAttributes.get(label), studioRole);
			}
		}
		set.getAnnotations().addAll(tableObject.getAnnotations());
		set.setSource(tableObject.getSource());
		return set;
	}

	/**
	 * Converts a table object into an example set sequentially in case no operator is known. If possible, {@link
	 * #convert(IOTable, ConcurrencyContext)} should be preferred.
	 *
	 * @param tableObject
	 * 		the table object to convert
	 * @return the example set
	 */
	public static ExampleSet convertSequentially(IOTable tableObject) {
		if (tableObject == null) {
			throw new IllegalArgumentException("Table object must not be null");
		}

		Table table = tableObject.getTable();
		List<Attribute> attributes = new ArrayList<>();
		List<String> labels = table.labels();
		int i = 0;
		for (String label : labels) {
			int valueType = getValueType(table, label, i);
			attributes.add(AttributeFactory.createAttribute(label, valueType));
			i++;
		}

		ExampleSet set = ExampleSets.from(attributes).withBlankSize(table.height()).build();
		convertSequentially(table, set);
		Attributes allAttributes = set.getAttributes();
		for (String label : labels) {
			String studioRole = convertRole(table, label);
			if (studioRole != null && checkUnique(allAttributes, studioRole)) {
				allAttributes.setSpecialAttribute(allAttributes.get(label), studioRole);
			}
		}
		set.getAnnotations().addAll(tableObject.getAnnotations());
		set.setSource(tableObject.getSource());
		return set;
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
	 * Roles for ExampleSets must be unique. If the converted roles are not, we need to make them. For now we ignore
	 * non-unique roles.
	 */
	private static boolean checkUnique(Attributes allAttributes, String studioRole) {
		boolean unusedRole = allAttributes.findRoleBySpecialName(studioRole) == null;
		if (!unusedRole) {
			LogService.getRoot().warning(() -> "Second occurence of role '" + studioRole + "' is dropped since roles " +
					"in ExampleSets must be unique");
		}
		return unusedRole;
	}

	/**
	 * Converts attribute roles into belt column roles.
	 */
	private static ColumnRole convert(String studioRole) {
		switch (studioRole) {
			case Attributes.LABEL_NAME:
				return ColumnRole.LABEL;
			case Attributes.ID_NAME:
				return ColumnRole.ID;
			case Attributes.PREDICTION_NAME:
				return ColumnRole.PREDICTION;
			case Attributes.CONFIDENCE_NAME:
				return ColumnRole.SCORE;
			case Attributes.CLUSTER_NAME:
				return ColumnRole.CLUSTER;
			case Attributes.OUTLIER_NAME:
				return ColumnRole.OUTLIER;
			case Attributes.WEIGHT_NAME:
				return ColumnRole.WEIGHT;
			case Attributes.BATCH_NAME:
				return ColumnRole.BATCH;
			default:
				if (studioRole.startsWith(Attributes.CONFIDENCE_NAME)) {
					return ColumnRole.SCORE;
				}
				return ColumnRole.METADATA;
		}
	}


	/**
	 * Converts the belt table role for the given label to an attribute role name.
	 *
	 * @param table
	 * 		the table to consider
	 * @param label
	 * 		the name of the column
	 * @return the legacy role name
	 */
	public static String convertRole(Table table, String label) {
		ColumnRole role = table.getFirstMetaData(label, ColumnRole.class);
		if (role == null) {
			// Nothing to convert, abort...
			return null;
		}
		String convertedRole;
		switch (role) {
			case LABEL:
				convertedRole = Attributes.LABEL_NAME;
				break;
			case ID:
				convertedRole = Attributes.ID_NAME;
				break;
			case PREDICTION:
				convertedRole = Attributes.PREDICTION_NAME;
				break;
			case CLUSTER:
				convertedRole = Attributes.CLUSTER_NAME;
				break;
			case OUTLIER:
				convertedRole = Attributes.OUTLIER_NAME;
				break;
			case WEIGHT:
				convertedRole = Attributes.WEIGHT_NAME;
				break;
			case BATCH:
				convertedRole = Attributes.BATCH_NAME;
				break;
			default:
				convertedRole = null;
				break;
		}

		if (convertedRole == null) {
			// no definite match for role, take legacy role into account
			LegacyRole legacyRole = table.getFirstMetaData(label, LegacyRole.class);
			if (legacyRole != null) {
				return legacyRole.role();
			} else if (role == ColumnRole.SCORE) {
				ColumnReference reference = table.getFirstMetaData(label, ColumnReference.class);
				if (reference != null && reference.getValue() != null) {
					return CONFIDENCE_PREFIX + reference.getValue();
				} else {
					return Attributes.CONFIDENCE_NAME;
				}

			} else if (role == ColumnRole.METADATA) {
				return META_DATA_NAME;
			}
		}
		return convertedRole;
	}

	/**
	 * Gets the value type from the meta data if present or from the table otherwise.
	 */
	static int getValueType(Table table, String label, int columnIndex) {
		Column column = table.column(columnIndex);
		int derivedOntology = convertToOntology(column);
		LegacyType legacyType = table.getFirstMetaData(label, LegacyType.class);
		if (legacyType != null) {
			int legacyOntology = legacyType.ontology();
			if (isAssignable(legacyOntology, derivedOntology, column)) {
				return legacyOntology;
			}
		}
		return derivedOntology;
	}

	/**
	 * Check if conversion to the legacy type is possible.
	 */
	private static boolean isAssignable(int legacyOntology, int derivedOntology, Column column) {
		// legacy ontology is super type or the same
		if (Ontology.ATTRIBUTE_VALUE_TYPE.isA(derivedOntology, legacyOntology)) {
			return true;
		}
		// if binominal is requested for a polynominal derived type, check dictionary size and if only positive
		if (legacyOntology == Ontology.BINOMINAL && derivedOntology == Ontology.POLYNOMINAL) {
			Dictionary<String> dictionary = column.getDictionary(String.class);
			return dictionary.size() <= 2 &&
					//BinominalMapping can have no positive but not no negative
					!(dictionary.isBoolean() && dictionary.hasPositive() && !dictionary.hasNegative());
		}
		// derived ontology is a nominal subtype and legacy ontology, too
		if (Ontology.ATTRIBUTE_VALUE_TYPE.isA(derivedOntology, Ontology.NOMINAL) && Ontology.ATTRIBUTE_VALUE_TYPE
				.isA(legacyOntology, Ontology.NOMINAL)) {
			return true;
		}
		// for legacy support we allow conversion from date-time to time
		if (legacyOntology == Ontology.TIME && derivedOntology == Ontology.DATE_TIME) {
			return true;
		}
		// date-time can be shown as date
		return legacyOntology == Ontology.DATE && derivedOntology == Ontology.DATE_TIME;
	}

	/**
	 * Copies the data from the table into the set sequentially.
	 */
	private static void convertSequentially(Table table, ExampleSet set) {
		int i = 0;
		for (Attribute attribute : set.getAttributes()) {
			Column column = table.column(i++);
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
				example.setValue(attribute, read.toEpochMilli());
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

		Dictionary<String> dictionary = column.getDictionary(String.class);
		List<String> mapping = ColumnAccessor.get().getDictionaryList(dictionary);
		if (dictionary.isBoolean()) {
			// check if last value is positive
			if (dictionary.getPositiveIndex() == 2 || !dictionary.hasPositive()) {
				copyNegativePositive(set, attribute, column, dictionary);
			} else {
				copyPositiveNegative(set, attribute, column, mapping);
			}
		}else{
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
	private static void copyNegativePositive(ExampleSet set, Attribute attribute, Column column, Dictionary<String>
			mapping) {
		NominalMapping legacyMapping = attribute.getMapping();
		//the first mapped value is negative, the order is kept
		for (Dictionary.Entry<String> value : mapping) {
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
	 * Finds the right {@link Ontology} for a given {@link Column}
	 *
	 * @param column
	 *            the column to convert
	 * @return the associated ontology
	 */
	public static int convertToOntology(Column column) {
		switch (column.type().id()) {
			case INTEGER:
				return Ontology.INTEGER;
			case REAL:
				return Ontology.REAL;
			case NOMINAL:
				Dictionary<Object> dictionary = column.getDictionary(Object.class);
				if (dictionary.isBoolean() && !(dictionary.hasPositive() && !dictionary.hasNegative())) {
					return Ontology.BINOMINAL;
				}
				return Ontology.POLYNOMINAL;
			case DATE_TIME:
				return Ontology.DATE_TIME;
			case TIME:
				//because of time zone issues, we cannot convert to time
				return Ontology.INTEGER;
			default:
				throw new UnsupportedOperationException(MESSAGE_NON_SUPPORTED);
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
			Throwable cause = e.getCause();
			if (cause instanceof RuntimeException) {
				throw (RuntimeException) cause;
			} else if (cause instanceof Error) {
				throw (Error) cause;
			} else {
				throw new RuntimeException(cause.getMessage(), cause);
			}
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
				columnTable.getDataRow(row).set(attribute, read.toEpochMilli());
			}
		}
		return null;
	}

	private static Void copyBinominalColumnToRows(ColumnarExampleTable columnTable, Attribute attribute,
												  Column column) {
		column = removeGapsFromDictionary(column);

		Dictionary<String> dictionary = column.getDictionary(String.class);
		if(dictionary.isBoolean()) {
			List<String> mapping = ColumnAccessor.get().getDictionaryList(dictionary);
			// check if last value is positive
			if (dictionary.getPositiveIndex() == 2 || !dictionary.hasPositive()) {
				copyNegativePositiveToRows(columnTable, attribute, column, dictionary);
			} else {
				copyPositiveNegativeToRows(columnTable, attribute, column, mapping);

			}
		}else{
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
												   Column column, Dictionary<String> mapping) {
		//the first mapped value is negative, the order is kept
		NominalMapping legacyMapping = attribute.getMapping();
		for (Dictionary.Entry<String> value : mapping) {
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
		List<String> mapping = ColumnAccessor.get().getDictionaryList(column.getDictionary(String.class));
		NominalMapping legacyMapping = attribute.getMapping();
		for (int j = 1; j < mapping.size(); j++) {
			legacyMapping.mapString(mapping.get(j));
		}
	}

	/**
	 * Conversion where the exampleSet cannot be accessed in parallel.
	 */
	private static Table sequentialConvert(ExampleSet exampleSet, ConcurrencyContext context) {
		int size = exampleSet.size();
		TableBuilder builder = Builders.newTableBuilder(size);
		Attribute prediction = exampleSet.getAttributes().getPredictedLabel();
		for (Iterator<AttributeRole> allRoles = exampleSet.getAttributes().allAttributeRoles(); allRoles.hasNext(); ) {
			AttributeRole role = allRoles.next();
			Attribute attribute = role.getAttribute();
			copyDataAndType(builder, exampleSet, size, attribute);
			if (role.isSpecial()) {
				String specialName = role.getSpecialName();
				ColumnRole beltRole = convert(specialName);
				builder.addMetaData(attribute.getName(), beltRole);
				if (beltRole == ColumnRole.METADATA) {
					builder.addMetaData(attribute.getName(), new LegacyRole(specialName));
				} else if (beltRole == ColumnRole.SCORE) {
					String predictionName = prediction == null ? null : prediction.getName();
					if (specialName.startsWith(CONFIDENCE_PREFIX)) {
						builder.addMetaData(attribute.getName(),
								new ColumnReference(predictionName,
										specialName.substring(CONFIDENCE_PREFIX_LENGHT)));
					} else {
						builder.addMetaData(attribute.getName(), new ColumnReference(predictionName));
					}
				}
			}
		}
		return builder.build(ContextAdapter.adapt(context));
	}

	/**
	 * Copies the data from the example set to the builder and adds a legacy type if the type is not determined by the
	 * data.
	 */
	private static void copyDataAndType(TableBuilder builder, ExampleSet exampleSet, int size, Attribute attribute) {
		String name = attribute.getName();
		switch (attribute.getValueType()) {
			case Ontology.NUMERICAL:
				builder.add(name, getRealColumn(exampleSet, size, attribute));
				builder.addMetaData(name, LegacyType.NUMERICAL);
				break;
			case Ontology.REAL:
				builder.add(name, getRealColumn(exampleSet, size, attribute));
				break;
			case Ontology.INTEGER:
				builder.add(name, getIntegerColumn(exampleSet, size, attribute));
				break;
			case Ontology.BINOMINAL:
				CategoricalColumn<String> binominalColumn = getBinominalColumn(exampleSet, size, attribute);
				builder.add(name, binominalColumn);
				builder.addMetaData(name, LegacyType.BINOMINAL);
				break;
			case Ontology.NOMINAL:
				builder.add(name, getNominalColumn(exampleSet, size, attribute));
				builder.addMetaData(name, LegacyType.NOMINAL);
				break;
			case Ontology.POLYNOMINAL:
				builder.add(name, getNominalColumn(exampleSet, size, attribute));
				break;
			case Ontology.STRING:
				builder.add(name, getNominalColumn(exampleSet, size, attribute));
				builder.addMetaData(name, LegacyType.STRING);
				break;
			case Ontology.FILE_PATH:
				builder.add(name, getNominalColumn(exampleSet, size, attribute));
				builder.addMetaData(name, LegacyType.FILE_PATH);
				break;
			case Ontology.DATE:
				builder.add(name, getDateColumn(exampleSet, size, attribute));
				builder.addMetaData(name, LegacyType.DATE);
				break;
			case Ontology.DATE_TIME:
				builder.add(name, getDateTimeColumn(exampleSet, size, attribute));
				break;
			case Ontology.TIME:
				builder.add(name, getDateTimeColumn(exampleSet, size, attribute));
				builder.addMetaData(name, LegacyType.TIME);
				break;
			default:
				throw new UnsupportedOperationException(MESSAGE_NON_SUPPORTED);
		}
	}


	private static Column getDateTimeColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, true, false);
		int i = 0;
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i++, null);
			} else {
				long longValue = (long) value;
				buffer.set(i++, Math.floorDiv(longValue, MILLISECONDS_PER_SECOND),
						(int) (Math.floorMod(longValue, MILLISECONDS_PER_SECOND) * NANOS_PER_MILLI_SECOND));
			}
		}
		return buffer.toColumn();
	}

	private static Column getDateColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, false, false);
		int i = 0;
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i++, null);
			} else {
				buffer.set(i++, ((long) value) / MILLISECONDS_PER_SECOND);
			}
		}
		return buffer.toColumn();
	}

	private static Column getIntegerColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		NumericBuffer intBuffer = Buffers.integerBuffer(size, false);
		int j = 0;
		for (Example example : exampleSet) {
			intBuffer.set(j++, example.getValue(attribute));
		}
		return intBuffer.toColumn();
	}

	private static Column getRealColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		NumericBuffer buffer = Buffers.realBuffer(size, false);
		int i = 0;
		for (Example example : exampleSet) {
			buffer.set(i++, example.getValue(attribute));
		}
		return buffer.toColumn();
	}

	/**
	 * Copies a binominal column from the example set by copying the mapping and the category data with a fallback in
	 * case the mapping is broken (contains null). Creates a boolean column if possible.
	 */
	private static CategoricalColumn<String> getBinominalColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		NominalMapping legacyMapping = attribute.getMapping();
		if (legacyMapping.getPositiveString() != null && (legacyMapping.getNegativeString() == null
				|| legacyMapping.getPositiveString().equals(legacyMapping.getNegativeString()))) {
			// Incompatible with Belt's 2Bit columns
			return getBufferColumn(exampleSet, size, attribute);
		}
		List<String> mapping = new ArrayList<>(3);
		mapping.add(null);
		String negativeString = legacyMapping.getNegativeString();
		if (negativeString != null) {
			mapping.add(negativeString);
		}
		String positiveString = legacyMapping.getPositiveString();
		if (positiveString != null) {
			mapping.add(positiveString);
		}
		byte[] data = new byte[size % 4 == 0 ? size / 4 : size / 4 + 1];

		int i = 0;
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (!Double.isNaN(value)) {
				IntegerFormats.writeUInt2(data, i, (int) (value + 1));
			}
			i++;
		}

		PackedIntegers packed = new PackedIntegers(data, Format.UNSIGNED_INT2, size);
		//convert to a boolean column
		int positiveIndex = legacyMapping.getPositiveIndex() + 1;
		if (positiveIndex >= mapping.size()) {
			//there is no positive value, only a negative one
			positiveIndex = BooleanDictionary.NO_ENTRY;
		}
		return ColumnAccessor.get().newCategoricalColumn(ColumnTypes.NOMINAL, packed, mapping, positiveIndex);
	}

	/**
	 * Copies a nominal column from the example set by copying the mapping and the category data with a fallback in case
	 * the mapping is broken (contains null or contains a value twice).
	 */
	private static CategoricalColumn<String> getNominalColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		NominalMapping legacyMapping = attribute.getMapping();
		List<String> mapping = new ArrayList<>(legacyMapping.size() + 1);
		mapping.add(null);
		Set<String> controlSet = new HashSet<>();
		controlSet.add(null);
		for (String value : legacyMapping.getValues()) {
			if (controlSet.add(value)) {
				mapping.add(value);
			} else {
				return getBufferColumn(exampleSet, size, attribute);
			}
		}
		int[] data = new int[size];
		int i = 0;
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (Double.isNaN(value)) {
				data[i++] = 0;
			} else {
				data[i++] = (int) value + 1;
			}
		}
		return ColumnAccessor.get().newCategoricalColumn(ColumnTypes.NOMINAL, data, mapping);
	}

	/**
	 * Copies a nominal column from the example set using a nominal buffer.
	 */
	private static CategoricalColumn<String> getBufferColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		CategoricalBuffer<String> nominalBuffer = BufferAccessor.get().newInt32Buffer(size);
		int j = 0;
		NominalMapping mapping = attribute.getMapping();
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (Double.isNaN(value)) {
				nominalBuffer.set(j++, null);
			} else {
				nominalBuffer.set(j++, mapping.mapIndex((int) value));
			}
		}
		return nominalBuffer.toColumn(ColumnTypes.NOMINAL);
	}

	/**
	 * Conversion where the exampleSet can be accessed in parallel.
	 */
	private static Table parallelConvert(ExampleSet exampleSet, ConcurrencyContext context) {
		int size = exampleSet.size();
		List<String> labels = new ArrayList<>();
		List<Callable<Column>> futureColumns = new ArrayList<>();
		Map<String, List<ColumnMetaData>> meta = new HashMap<>();
		Attribute predictionAttribute = exampleSet.getAttributes().getPredictedLabel();
		for (Iterator<AttributeRole> allRoles = exampleSet.getAttributes().allAttributeRoles(); allRoles.hasNext(); ) {
			AttributeRole role = allRoles.next();

			Attribute attribute = role.getAttribute();
			labels.add(attribute.getName());

			createDataFuturesAndStoreType(exampleSet, size, futureColumns, attribute, meta);

			if (role.isSpecial()) {
				storeRole(role, attribute, meta, predictionAttribute);
			}
		}
		return buildTable(futureColumns, labels, meta, context);
	}

	/**
	 * Converts the column specified by exampleSet and attribute into one entry in futureColumns and meta.
	 */
	private static void createDataFuturesAndStoreType(ExampleSet exampleSet, int size,
													  List<Callable<Column>> futureColumns, Attribute attribute,
													  Map<String, List<ColumnMetaData>> meta) {
		switch (attribute.getValueType()) {
			case Ontology.NUMERICAL:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getRealColumn(exampleSet, size, attribute));
				break;
			case Ontology.REAL:
				futureColumns.add(() -> getRealColumn(exampleSet, size, attribute));
				break;
			case Ontology.INTEGER:
				futureColumns.add(() -> getIntegerColumn(exampleSet, size, attribute));
				break;
			case Ontology.BINOMINAL:
				futureColumns.add(() -> getBinominalColumn(exampleSet, size, attribute));
				storeOntology(meta, attribute);
				break;
			case Ontology.POLYNOMINAL:
				futureColumns.add(() -> getNominalColumn(exampleSet, size, attribute));
				break;
			case Ontology.NOMINAL:
			case Ontology.STRING:
			case Ontology.FILE_PATH:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getNominalColumn(exampleSet, size, attribute));
				break;
			case Ontology.DATE:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getDateColumn(exampleSet, size, attribute));
				break;
			case Ontology.DATE_TIME:
				futureColumns.add(() -> getDateTimeColumn(exampleSet, size, attribute));
				break;
			case Ontology.TIME:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getDateTimeColumn(exampleSet, size, attribute));
				break;
			default:
				throw new UnsupportedOperationException(MESSAGE_NON_SUPPORTED);
		}
	}

	/**
	 * Stores the ontology of the attribute in the meta map under the attribute name.
	 */
	private static void storeOntology(Map<String, List<ColumnMetaData>> meta, Attribute attribute) {
		List<ColumnMetaData> list = new ArrayList<>(3);
		list.add(LegacyType.forOntology(attribute.getValueType()));
		meta.put(attribute.getName(), list);
	}

	/**
	 * Conversion where the data is read directly from the example table and in parallel.
	 */
	private static Table exampleTableConvert(ExampleSet exampleSet, ConcurrencyContext context) {
		int size = exampleSet.size();
		List<String> labels = new ArrayList<>();
		List<Callable<Column>> futureColumns = new ArrayList<>();
		Map<String, List<ColumnMetaData>> meta = new HashMap<>();
		ExampleTable table = exampleSet.getExampleTable();
		Attribute prediction = exampleSet.getAttributes().getPredictedLabel();
		for (Iterator<AttributeRole> allRoles = exampleSet.getAttributes().allAttributeRoles(); allRoles.hasNext(); ) {

			AttributeRole role = allRoles.next();
			Attribute attribute = role.getAttribute();
			labels.add(attribute.getName());

			createTableFuturesAndStoreType(size, futureColumns, meta, table, attribute);
			if (role.isSpecial()) {
				storeRole(role, attribute, meta, prediction);
			}
		}
		return buildTable(futureColumns, labels, meta, context);
	}

	/**
	 * Stores the associated belt role and, if not all the info can be captured by the belt role, stores the original
	 * role name.
	 */
	private static void storeRole(AttributeRole role, Attribute attribute, Map<String, List<ColumnMetaData>> meta,
								  Attribute prediction) {
		String specialName = role.getSpecialName();
		ColumnRole beltRole = convert(specialName);
		List<ColumnMetaData> columnMeta =
				meta.computeIfAbsent(attribute.getName(), s -> new ArrayList<>(2));
		columnMeta.add(beltRole);
		if (beltRole == ColumnRole.METADATA) {
			columnMeta.add(new LegacyRole(specialName));
		} else if (beltRole == ColumnRole.SCORE) {
			String predictionName = prediction == null ? null : prediction.getName();
			if (specialName.startsWith(CONFIDENCE_PREFIX)) {
				columnMeta.add(new ColumnReference(predictionName,
						specialName.substring(CONFIDENCE_PREFIX_LENGHT)));
			} else {
				columnMeta.add(new ColumnReference(predictionName));
			}
		}
	}

	/**
	 * Converts the column specified by table and attribute into one entry in futureColumns and meta.
	 */
	private static void createTableFuturesAndStoreType(int size, List<Callable<Column>> futureColumns,
													   Map<String, List<ColumnMetaData>> meta, ExampleTable table,
													   Attribute attribute) {
		switch (attribute.getValueType()) {
			case Ontology.NUMERICAL:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getRealColumn(size, table, attribute));
				break;
			case Ontology.REAL:
				futureColumns.add(() -> getRealColumn(size, table, attribute));
				break;
			case Ontology.INTEGER:
				futureColumns.add(() -> getIntegerColumn(size, table, attribute));
				break;
			case Ontology.BINOMINAL:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getBinominalColumn(table, size, attribute));
				break;
			case Ontology.POLYNOMINAL:
				futureColumns.add(() -> getNominalColumn(table, size, attribute));
				break;
			case Ontology.NOMINAL:
			case Ontology.STRING:
			case Ontology.FILE_PATH:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getNominalColumn(table, size, attribute));
				break;
			case Ontology.DATE:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getSecondDateColumn(size, table, attribute));
				break;
			case Ontology.DATE_TIME:
				futureColumns.add(() -> getNanosecondDateColumn(size, table, attribute));
				break;
			case Ontology.TIME:
				storeOntology(meta, attribute);
				futureColumns.add(() -> getNanosecondDateColumn(size, table, attribute));
				break;
			default:
				throw new UnsupportedOperationException(MESSAGE_NON_SUPPORTED);
		}
	}

	private static Column getNanosecondDateColumn(int size, ExampleTable table, Attribute attribute) {
		DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, true, false);
		for (int i = 0; i < table.size(); i++) {
			double value = table.getDataRow(i).get(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i, null);
			} else {
				long longValue = (long) value;
				buffer.set(i, Math.floorDiv(longValue, MILLISECONDS_PER_SECOND),
						(int) (Math.floorMod(longValue, MILLISECONDS_PER_SECOND) * NANOS_PER_MILLI_SECOND));
			}
		}
		return buffer.toColumn();
	}

	private static Column getSecondDateColumn(int size, ExampleTable table, Attribute attribute) {
		DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, false, false);
		for (int i = 0; i < table.size(); i++) {
			double value = table.getDataRow(i).get(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i, null);
			} else {
				buffer.set(i, ((long) value) / MILLISECONDS_PER_SECOND);
			}
		}
		return buffer.toColumn();
	}

	private static Column getRealColumn(int size, ExampleTable table, Attribute attribute) {
		NumericBuffer buffer = Buffers.realBuffer(size, false);
		for (int i = 0; i < table.size(); i++) {
			buffer.set(i, table.getDataRow(i).get(attribute));
		}
		return buffer.toColumn();
	}

	private static Column getIntegerColumn(int size, ExampleTable table, Attribute attribute) {
		NumericBuffer intBuffer = Buffers.integerBuffer(size, false);
		for (int i = 0; i < table.size(); i++) {
			intBuffer.set(i, table.getDataRow(i).get(attribute));
		}
		return intBuffer.toColumn();
	}

	/**
	 * Copies a binominal column from the example table by copying the mapping and the category data with a fallback in
	 * case the mapping is broken (contains null).
	 */
	private static Column getBinominalColumn(ExampleTable table, int size, Attribute attribute) {
		NominalMapping legacyMapping = attribute.getMapping();
		if (legacyMapping.getPositiveString() != null && (legacyMapping.getNegativeString() == null
				|| legacyMapping.getPositiveString().equals(legacyMapping.getNegativeString()))) {
			// Incompatible with Belt's 2Bit columns
			return getBufferColumn(table, size, attribute);
		}
		List<String> mapping = new ArrayList<>(3);
		mapping.add(null);
		String negativeString = legacyMapping.getNegativeString();
		if (negativeString != null) {
			mapping.add(negativeString);
		}
		String positiveString = legacyMapping.getPositiveString();
		if (positiveString != null) {
			mapping.add(positiveString);
		}
		byte[] data = new byte[size % 4 == 0 ? size / 4 : size / 4 + 1];

		for (int i = 0; i < size; i++) {
			double value = table.getDataRow(i).get(attribute);
			if (!Double.isNaN(value)) {
				IntegerFormats.writeUInt2(data, i, (int) value + 1);
			}
		}

		PackedIntegers packed = new PackedIntegers(data, Format.UNSIGNED_INT2, size);
		// create boolean column
		int positiveIndex = legacyMapping.getPositiveIndex() + 1;
		if (positiveIndex >= mapping.size()) {
			//there is no positive value, only a negative one
			positiveIndex = BooleanDictionary.NO_ENTRY;
		}
		return ColumnAccessor.get().newCategoricalColumn(ColumnTypes.NOMINAL, packed, mapping, positiveIndex);
	}

	/**
	 * Copies a nominal column from the example table by copying the mapping and the category data with a fallback in
	 * case the mapping is broken (contains null or contains a value twice).
	 */
	private static Column getNominalColumn(ExampleTable table, int size, Attribute attribute) {
		NominalMapping legacyMapping = attribute.getMapping();
		List<String> mapping = new ArrayList<>(legacyMapping.size() + 1);
		mapping.add(null);
		Set<String> controlSet = new HashSet<>();
		controlSet.add(null);
		for (String value : legacyMapping.getValues()) {
			if (controlSet.add(value)) {
				mapping.add(value);
			} else {
				return getBufferColumn(table, size, attribute);
			}
		}
		int[] data = new int[size];
		for (int i = 0; i < size; i++) {
			double value = table.getDataRow(i).get(attribute);
			if (Double.isNaN(value)) {
				data[i] = 0;
			} else {
				data[i] = (int) value + 1;
			}
		}
		return ColumnAccessor.get().newCategoricalColumn(ColumnTypes.NOMINAL, data, mapping);
	}

	/**
	 * Copies a nominal column from the example table using a nominal buffer.
	 */
	private static Column getBufferColumn(ExampleTable table, int size, Attribute attribute) {
		CategoricalBuffer<String> nominalBuffer = BufferAccessor.get().newInt32Buffer(size);
		NominalMapping mapping = attribute.getMapping();
		for (int i = 0; i < size; i++) {
			double value = table.getDataRow(i).get(attribute);
			if (Double.isNaN(value)) {
				nominalBuffer.set(i, null);
			} else {
				nominalBuffer.set(i, mapping.mapIndex((int) value));
			}
		}
		return nominalBuffer.toColumn(ColumnTypes.NOMINAL);
	}

	/**
	 * Builds the table by running the future columns in the given context and creating a table from the results and
	 * the given labels.
	 */
	private static Table buildTable(List<Callable<Column>> futureColumns, List<String> labels,
									Map<String, List<ColumnMetaData>> srcMeta, ConcurrencyContext context) {
		try {
			List<Column> columnList = context.call(futureColumns);
			return new Table(columnList.toArray(new Column[0]),
					labels.toArray(new String[0]), srcMeta);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof RuntimeException) {
				throw (RuntimeException) cause;
			} else if (cause instanceof Error) {
				throw (Error) cause;
			} else {
				throw new RuntimeException(cause.getMessage(), cause);
			}
		}
	}
}
