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

import static com.rapidminer.belt.table.BeltConverter.CONFIDENCE_PREFIX;
import static com.rapidminer.belt.table.BeltConverter.IOOBJECT_USER_DATA_COLUMN_META_DATA_KEY;
import static com.rapidminer.belt.table.BeltConverter.MILLIS_PER_SECOND;
import static com.rapidminer.belt.table.BeltConverter.NANOS_PER_MILLI_SECOND;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
import com.rapidminer.belt.buffer.DateTimeBuffer;
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.buffer.TimeBuffer;
import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.column.TimeColumn;
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
import com.rapidminer.example.set.MappingBasedExampleSet;
import com.rapidminer.example.set.SimpleExampleSet;
import com.rapidminer.example.table.BinominalAttribute;
import com.rapidminer.example.table.DateAttribute;
import com.rapidminer.example.table.ExampleTable;
import com.rapidminer.example.table.NominalMapping;
import com.rapidminer.example.table.NumericalAttribute;
import com.rapidminer.example.table.PolynominalAttribute;
import com.rapidminer.example.table.internal.ColumnarExampleTable;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.Tools;


/**
 * Converts between from {@link ExampleSet}s to belt {@link Table}s.
 *
 * @author Gisa Meier
 * @since 0.7
 */
enum ToTableConverter {
	; //No instance enum

	/**
	 * Message for when unknown ontologies
	 */
	private static final String MESSAGE_UNKNOWN_TYPE = "Unknown attribute type";

	/**
	 * Set of primitive attribute types that are known to be thread safe for read accesses.
	 */
	private static final Set<Class<? extends Attribute>> SAFE_ATTRIBUTES = new HashSet<>(5);

	/**
	 * The length of the {@link BeltConverter#CONFIDENCE_PREFIX}
	 */
	private static final int CONFIDENCE_PREFIX_LENGTH = CONFIDENCE_PREFIX.length();

	private static final Column EMPTY_REAL_COLUMN = Buffers.realBuffer(0).toColumn();
	private static final Column EMPTY_INT_COLUMN = Buffers.integer53BitBuffer(0).toColumn();
	private static final DateTimeColumn EMPTY_DATE_COLUMN = Buffers.dateTimeBuffer(0, false).toColumn();
	private static final DateTimeColumn EMPTY_DATETIME_COLUMN = Buffers.dateTimeBuffer(0, true).toColumn();
	private static final TimeColumn EMPTY_TIME_COLUMN = Buffers.timeBuffer(0).toColumn();

	static {
		SAFE_ATTRIBUTES.add(DateAttribute.class);
		SAFE_ATTRIBUTES.add(BinominalAttribute.class);
		SAFE_ATTRIBUTES.add(PolynominalAttribute.class);
		SAFE_ATTRIBUTES.add(NumericalAttribute.class);
	}

	/**
	 * Creates a belt {@link IOTable} from the given {@link ExampleSet}. This is done in parallel if the exampleSet is
	 * threadsafe.
	 *
	 * @param exampleSet
	 * 		the exampleSet to convert
	 * @param context
	 * 		the concurrency context to use for the conversion
	 * @return a belt table
	 */
	static IOTable convert(ExampleSet exampleSet, ConcurrencyContext context) {
		if (exampleSet == null) {
			throw new IllegalArgumentException("Example set must not be null");
		}
		if (context == null) {
			throw new IllegalArgumentException("Context must not be null");
		}

		// handle the special case where there are no columns, but a height
		if (exampleSet.getAttributes().allSize() == 0 && exampleSet.size() > 0) {
			return new IOTable(new Table(exampleSet.size()));
		}

		// check if the example set is a wrapped belt table with a simple or stacked mapped views on top
		if (getExampleTable(exampleSet) instanceof ConvertOnWriteExampleTable) {
			if (exampleSet instanceof SimpleExampleSet) {
				return convertWrapped(exampleSet, (SimpleExampleSet) exampleSet, context);
			}

			if (exampleSet instanceof MappingBasedExampleSet) {
				ExampleSet testSet = getHighestParent(exampleSet);
				if (testSet instanceof SimpleExampleSet) {
					return convertWrapped(exampleSet, (SimpleExampleSet) testSet, context);
				}
			}
			//cannot reuse underlying columns, use default conversion
		}
		return defaultConvert(exampleSet, context);

	}

	/**
	 * Iteratively get the parent if the current is mapping based and the next mapping based or simple.
	 */
	private static ExampleSet getHighestParent(ExampleSet exampleSet) {
		ExampleSet testSet = exampleSet;
		while (testSet instanceof MappingBasedExampleSet && ((MappingBasedExampleSet) testSet).isParentSimpleOrMapped()) {
			testSet = ((MappingBasedExampleSet) testSet).getParentClone();
		}
		return testSet;
	}

	/**
	 * Checks if conversion can be done directly on the underlying {@link ExampleTable} or in parallel on the {@link
	 * ExampleSet} or must be done sequentially. Then does the conversion.
	 */
	private static IOTable defaultConvert(ExampleSet exampleSet, ConcurrencyContext context) {
		// check example set implementation
		boolean threadSafe = exampleSet instanceof AbstractExampleSet
				&& ((AbstractExampleSet) exampleSet).isThreadSafeView();

		// check example table implementation
		if (threadSafe) {
			ExampleTable table = getExampleTable(exampleSet);
			threadSafe = table instanceof ColumnarExampleTable;
		}

		threadSafe = areAttributesThreadsafe(exampleSet, threadSafe);

		Table table = doFittingConversion(exampleSet, threadSafe, context);
		return createIOTable(exampleSet, table);
	}

	/**
	 * Does the conversion directly on the underlying {@link ExampleTable} or in parallel on the {@link ExampleSet} or
	 * sequentially depending on the threadSafe parameter.
	 */
	private static Table doFittingConversion(ExampleSet exampleSet, boolean threadSafe, ConcurrencyContext context) {
		Table table;
		if (threadSafe) {
			// we can safely read from the input example using multiple threads
			boolean simpleView = exampleSet.getClass() == SimpleExampleSet.class;
			if (simpleView) {
				// we can ignore the view and read directly from the underlying example table
				table = exampleTableConvert(exampleSet, context);
			} else {
				table = parallelConvert(exampleSet, context);
			}
		} else {
			table = sequentialConvert(exampleSet, context);
		}
		return table;
	}

	/**
	 * Creates a new {@link IOTable} for the given table with the annotations and source from the {@link ExampleSet}.
	 */
	private static IOTable createIOTable(ExampleSet exampleSet, Table table) {
		IOTable tableObject = new IOTable(table);
		tableObject.getAnnotations().addAll(exampleSet.getAnnotations());
		tableObject.setSource(exampleSet.getSource());
		return tableObject;
	}

	/**
	 * Checks if the attributes of the example set for thread safety (if the threadSafe parameter is not {@code false} anyway).
	 */
	private static boolean areAttributesThreadsafe(ExampleSet exampleSet, boolean threadSafe) {
		// check attribute implementation
		if (threadSafe) {
			threadSafe = areAttributesSimple(exampleSet);
		}

		// check individual attributes and attribute transformations
		if (threadSafe) {
			Iterator<Attribute> attributes = exampleSet.getAttributes().allAttributes();
			while (attributes.hasNext()) {
				Attribute attribute = attributes.next();
				if (attributeNotSafe(attribute)) {
					return false;
				}
			}
		}
		return threadSafe;
	}

	/**
	 * Checks whether the attribute is in the list of safe attributes and does not contain transformations.
	 */
	private static boolean attributeNotSafe(Attribute attribute) {
		return !SAFE_ATTRIBUTES.contains(attribute.getClass()) || attribute.getLastTransformation() != null;
	}

	/**
	 * Checks whether the attributes of the exampleSet are {@link SimpleAttributes}.
	 */
	private static boolean areAttributesSimple(ExampleSet exampleSet) {
		Attributes attributes = exampleSet.getAttributes();
		return attributes.getClass() == SimpleAttributes.class;
	}

	/**
	 * Converts the simpleOrMappingBased example set to an {@link IOTable}, reusing columns of the underlying belt
	 * {@link Table} whenever possible.
	 *
	 * @param simpleOrMappingBased
	 * 		the example set to convert that is either simple or stacked mapping based with a {@link
	 *        ConvertOnWriteExampleTable} underlying
	 * @param simpleParent
	 * 		the simple parent of the simpleOrMappingBased
	 * @param context
	 * 		the context to use
	 * @return the converted table
	 */
	private static IOTable convertWrapped(ExampleSet simpleOrMappingBased, SimpleExampleSet simpleParent,
										  ConcurrencyContext context) {
		ConvertOnWriteExampleTable exampleTable = (ConvertOnWriteExampleTable) simpleOrMappingBased.getExampleTable();
		ColumnarExampleTable newColumns = exampleTable.getNewColumns();
		Table table = exampleTable.getTable();
		boolean simpleAttributes = areAttributesSimple(simpleOrMappingBased);
		if (table == null || !simpleAttributes) {
			// cannot reuse wrapped table, use normal conversion
			Table newTable = doFittingConversion(simpleOrMappingBased, areAttributesThreadsafe(simpleOrMappingBased, simpleAttributes), context);
			return createIOTable(simpleOrMappingBased, newTable);
		}

		Table newTable = convertWithReuse(simpleOrMappingBased, simpleParent, newColumns, table, context);
		int[] mapping = null;
		ExampleSet testSet = simpleOrMappingBased;
		// go through all parents and collapse the mappings until reaching the simple example set
		while (testSet instanceof MappingBasedExampleSet && ((MappingBasedExampleSet) testSet).isParentSimpleOrMapped()) {
			if (mapping == null) {
				mapping = ((MappingBasedExampleSet) testSet).getMappingCopy();
			} else {
				mapping = collapseMappings(mapping, ((MappingBasedExampleSet) testSet).getMappingCopy());
			}
			testSet = ((MappingBasedExampleSet) testSet).getParentClone();
		}
		if (mapping != null) {
			newTable = newTable.map(mapping, true, ContextAdapter.adapt(context));
		}
		return createIOTable(simpleOrMappingBased, newTable);
	}

	/**
	 * Converts to a new belt table reusing columns from the given table if possible.
	 *
	 * @param attributeExampleSet
	 * 		the example set that determines the attributes
	 * @param simpleExampleSet
	 * 		the underlying simple example set, can be the same as the one above
	 * @param additionalColumns
	 * 		the additionally added columns, can be {@code null}
	 * @param table
	 * 		the belt table for the first columns
	 * @param context
	 * 		the context to use for copying in parallel
	 * @return the converted table
	 */
	private static Table convertWithReuse(ExampleSet attributeExampleSet, SimpleExampleSet simpleExampleSet,
										  ColumnarExampleTable additionalColumns, Table table, ConcurrencyContext context) {
		int width = attributeExampleSet.getAttributes().allSize();
		String[] labels = new String[width];
		Column[] columns = new Column[width];
		Map<String, List<ColumnMetaData>> metaData = new HashMap<>();
		Attribute prediction = attributeExampleSet.getAttributes().getPredictedLabel();

		List<Callable<Void>> columnCallables = new ArrayList<>();
		int newIndex = 0;
		for (Iterator<AttributeRole> attributeRoleIterator = attributeExampleSet.getAttributes().allAttributeRoles(); attributeRoleIterator.hasNext(); ) {
			AttributeRole next = attributeRoleIterator.next();
			Attribute attribute = next.getAttribute();
			int index = attribute.getTableIndex();

			if (attributeNotSafe(attribute)) {
				// must copy column sequentially
				createDataAndStoreType(simpleExampleSet, simpleExampleSet.size(), columns, newIndex, attribute, metaData);
			} else {
				if (index < table.width()) {
					// column is part of the old table, reuse it
					reuseColumn(table, columns, attribute, newIndex, metaData, index);
				} else {
					// column is part of the newColumns table, create callable to copy it
					Attribute shiftedAttribute = (Attribute) attribute.clone();
					shiftedAttribute.setTableIndex(shiftedAttribute.getTableIndex() - table.width());
					columnCallables.add(createTableCallablesAndStoreType(table.height(), metaData, additionalColumns,
							shiftedAttribute, columns, newIndex));
				}
			}

			labels[newIndex] = attribute.getName();
			if (next.isSpecial()) {
				storeRole(next, attribute, metaData, prediction);
			}
			newIndex++;
		}

		// if there are column callebles to copy from the newColumns table, execute them in parallel
		if (!columnCallables.isEmpty()) {
			try {
				context.call(columnCallables);
			} catch (ExecutionException e) {
				handleExecutionException(e);
			}
		}

		restoreBeltMetaDataFromExampleSetUserData(attributeExampleSet, metaData, new HashSet<>(Arrays.asList(labels)));
		return new Table(columns, labels, metaData);
	}

	/**
	 * Copies the column at position table index from the table and stores it at position index in the columns array.
	 * Stores the type if necessary in the meta data.
	 */
	private static void reuseColumn(Table table, Column[] columns, Attribute attribute, int newIndex,
									Map<String, List<ColumnMetaData>> metaData, int tableIndex) {
		Column column = table.column(tableIndex);
		if (integerChangedToReal(attribute, column)) {
			columns[newIndex] = Buffers.realBuffer(column).toColumn();
		} else {
			columns[newIndex] = column;
		}
		storeType(metaData, attribute);
	}

	/**
	 * Creates a callable that creates a column and writes it into the index-position of the column array. Also stores
	 * the ontology, if necessary.
	 */
	private static Callable<Void> createTableCallablesAndStoreType(int size, Map<String, List<ColumnMetaData>> meta,
																   ExampleTable table, Attribute attribute, Column[] columns,
																   int index) {
		switch (attribute.getValueType()) {
			case Ontology.NUMERICAL:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getRealColumn(size, table, attribute);
					return null;
				};
			case Ontology.REAL:
				return () -> {
					columns[index] = getRealColumn(size, table, attribute);
					return null;
				};
			case Ontology.INTEGER:
				return () -> {
					columns[index] = getIntegerColumn(size, table, attribute);
					return null;
				};
			case Ontology.BINOMINAL:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getBinominalColumn(table, size, attribute);
					return null;
				};
			case Ontology.NOMINAL:
				return () -> {
					columns[index] = getNominalColumn(table, size, attribute);
					return null;
				};
			case Ontology.POLYNOMINAL:
			case Ontology.STRING:
			case Ontology.FILE_PATH:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getNominalColumn(table, size, attribute);
					return null;
				};
			case Ontology.DATE:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getSecondDateColumn(size, table, attribute);
					return null;
				};
			case Ontology.DATE_TIME:
				return () -> {
					columns[index] = getNanosecondDateColumn(size, table, attribute);
					return null;
				};
			case Ontology.TIME:
				return () -> {
					columns[index] = getTimeColumn(table, size, attribute);
					return null;
				};
			default:
				throw new UnsupportedOperationException(MESSAGE_UNKNOWN_TYPE);
		}
	}


	/**
	 * Stores the ontologies for which it is necessary, same as in {@link #createTableCallablesAndStoreType(int, Map,
	 * ExampleTable, Attribute, Column[], int)}.
	 */
	private static void storeType(Map<String, List<ColumnMetaData>> meta, Attribute attribute) {
		if (!LegacyType.DIRECTLY_MAPPED_ONTOLOGIES.contains(attribute.getValueType())) {
			storeOntology(meta, attribute);
		}
	}

	/**
	 * Creates a new mapping from applying first mappingA and then mappingB.
	 */
	private static int[] collapseMappings(int[] mappingA, int[] mappingB) {
		int[] newMapping = new int[mappingA.length];
		for (int i = 0; i < newMapping.length; i++) {
			newMapping[i] = mappingB[mappingA[i]];
		}
		return newMapping;
	}

	/**
	 * Numeric to real only changes the ontology, not the data. If the column was integer and now is real, we need to
	 * copy because in belt the type cannot change.
	 */
	private static boolean integerChangedToReal(Attribute attribute, Column column) {
		return column.type().id() == Column.TypeId.INTEGER_53_BIT && attribute.getValueType() != Ontology.INTEGER && attribute.isNumerical();
	}

	/**
	 * Calls {@link ExampleSet#getExampleTable()} and returns {@code null} in case of an exception.
	 */
	static ExampleTable getExampleTable(ExampleSet exampleSet) {
		try {
			return exampleSet.getExampleTable();
		} catch (UnsupportedOperationException e) {
			//if exampleSet is a HeaderExampleSet we need to ignore the exception from getExampleTable()
			return null;
		}
	}


	/**
	 * Conversion where the exampleSet cannot be accessed in parallel.
	 */
	private static Table sequentialConvert(ExampleSet exampleSet, ConcurrencyContext context) {
		int size = exampleSet.size();
		Set<String> labels = new HashSet<>();
		TableBuilder builder = Builders.newTableBuilder(size);
		Attribute prediction = exampleSet.getAttributes().getPredictedLabel();
		for (Iterator<AttributeRole> allRoles = exampleSet.getAttributes().allAttributeRoles(); allRoles.hasNext(); ) {
			AttributeRole role = allRoles.next();
			Attribute attribute = role.getAttribute();
			labels.add(attribute.getName());
			copyDataAndType(builder, exampleSet, size, attribute);
			if (role.isSpecial()) {
				String specialName = role.getSpecialName();
				ColumnRole beltRole = BeltConverter.convertRole(specialName);
				builder.addMetaData(attribute.getName(), beltRole);
				if (beltRole == ColumnRole.METADATA) {
					builder.addMetaData(attribute.getName(), new LegacyRole(specialName));
				} else if (beltRole == ColumnRole.SCORE) {
					String predictionName = prediction == null ? null : prediction.getName();
					if (specialName.startsWith(CONFIDENCE_PREFIX)) {
						builder.addMetaData(attribute.getName(),
								new ColumnReference(predictionName,
										specialName.substring(CONFIDENCE_PREFIX_LENGTH)));
					} else {
						builder.addMetaData(attribute.getName(), new ColumnReference(predictionName));
						if (!Attributes.CONFIDENCE_NAME.equals(specialName)) {
							builder.addMetaData(attribute.getName(), new LegacyRole(specialName));
						}
					}
				}
			}
		}
		restoreBeltMetaDataFromExampleSetUserData(exampleSet, builder, labels);
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
				Column binominalColumn = getBinominalColumn(exampleSet, size, attribute);
				builder.add(name, binominalColumn);
				builder.addMetaData(name, LegacyType.BINOMINAL);
				break;
			case Ontology.NOMINAL:
				builder.add(name, getNominalColumn(exampleSet, size, attribute));
				break;
			case Ontology.POLYNOMINAL:
				builder.add(name, getNominalColumn(exampleSet, size, attribute));
				builder.addMetaData(name, LegacyType.POLYNOMINAL);
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
				builder.add(name, getTimeColumn(exampleSet, size, attribute));
				break;
			default:
				throw new UnsupportedOperationException(MESSAGE_UNKNOWN_TYPE);
		}
	}

	private static Column getDateTimeColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		if (size == 0) {
			return EMPTY_DATETIME_COLUMN;
		}
		DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, true, false);
		int i = 0;
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i++, null);
			} else {
				// in Java 8 there is only floorDiv(long,long) but in Java 11 there is also floorDiv(long,int) - so
				// force longs to have code compiled with Java 11 and target Java 8 actually run with Java 8
				long longValue = (long) value;
				long longDivisor = MILLIS_PER_SECOND;
				buffer.set(i++, Math.floorDiv(longValue, longDivisor),
						(int) (Math.floorMod(longValue, longDivisor) * NANOS_PER_MILLI_SECOND));
			}
		}
		return buffer.toColumn();
	}

	private static Column getTimeColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		if (size == 0) {
			return EMPTY_TIME_COLUMN;
		}
		TimeBuffer buffer = Buffers.timeBuffer(size, false);
		int i = 0;
		Calendar calendar = Tools.getPreferredCalendar();
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i++, null);
			} else {
				long nanos = BeltConverter.legacyTimeDoubleToNanoOfDay(value, calendar);
				buffer.set(i++, nanos);
			}
		}
		return buffer.toColumn();
	}

	private static Column getDateColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		if (size == 0) {
			return EMPTY_DATE_COLUMN;
		}
		DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, false, false);
		int i = 0;
		for (Example example : exampleSet) {
			double value = example.getValue(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i++, null);
			} else {
				buffer.set(i++, ((long) value) / MILLIS_PER_SECOND);
			}
		}
		return buffer.toColumn();
	}

	private static Column getIntegerColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		if (size == 0) {
			return EMPTY_INT_COLUMN;
		}
		NumericBuffer intBuffer = Buffers.integer53BitBuffer(size, false);
		int j = 0;
		for (Example example : exampleSet) {
			intBuffer.set(j++, example.getValue(attribute));
		}
		return intBuffer.toColumn();
	}

	private static Column getRealColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		if (size == 0) {
			return EMPTY_REAL_COLUMN;
		}
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
	private static Column getBinominalColumn(ExampleSet exampleSet, int size, Attribute attribute) {
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

		if (size > 0) {
			int i = 0;
			for (Example example : exampleSet) {
				double value = example.getValue(attribute);
				if (!Double.isNaN(value)) {
					IntegerFormats.writeUInt2(data, i, (int) (value + 1));
				}
				i++;
			}
		}

		PackedIntegers packed = new PackedIntegers(data, Format.UNSIGNED_INT2, size);
		//convert to a boolean column
		int positiveIndex = legacyMapping.getPositiveIndex() + 1;
		if (legacyMapping instanceof NominalMappingAdapter) {
			// no shift for adapter
			positiveIndex = legacyMapping.getPositiveIndex();
		}
		if (positiveIndex >= mapping.size()) {
			//there is no positive value, only a negative one
			positiveIndex = BooleanDictionary.NO_ENTRY;
		}
		return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, packed, mapping, positiveIndex);
	}

	/**
	 * Copies a nominal column from the example set by copying the mapping and the category data with a fallback in
	 * case
	 * the mapping is broken (contains null or contains a value twice).
	 */
	private static Column getNominalColumn(ExampleSet exampleSet, int size, Attribute attribute) {
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
		if (size > 0) {
			int i = 0;
			for (Example example : exampleSet) {
				double value = example.getValue(attribute);
				if (Double.isNaN(value)) {
					data[i++] = 0;
				} else {
					data[i++] = (int) value + 1;
				}
			}
		}
		return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, mapping);
	}

	/**
	 * Copies a nominal column from the example set using a nominal buffer.
	 */
	private static Column getBufferColumn(ExampleSet exampleSet, int size, Attribute attribute) {
		if (exampleSet instanceof HeaderExampleSet) {
			return handleWrongNominalHeader(attribute);
		}
		NominalBuffer nominalBuffer =
				BufferAccessor.get().newInt32Buffer(ColumnType.NOMINAL, size);
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
		return nominalBuffer.toColumn();
	}

	/**
	 * Handle the case where the nominal mapping of the attribute is broken in a way that is very rare but not
	 * impossible by the mapping api. For example, it could happen that a value appears more than once or that there
	 * are {@code null}s in the middle.
	 */
	private static Column handleWrongNominalHeader(Attribute attribute) {
		NominalMapping mapping = attribute.getMapping();
		List<String> values = mapping.getValues();
		NominalBuffer nominalBuffer = BufferAccessor.get().newInt32Buffer(ColumnType.NOMINAL, values.size());
		int i = 0;
		for (String value : values) {
			nominalBuffer.set(i++, value);
		}
		CategoricalColumn categoricalColumn = nominalBuffer.toColumn();
		return categoricalColumn.stripData();
	}

	/**
	 * Conversion where the exampleSet can be accessed in parallel.
	 */
	private static Table parallelConvert(ExampleSet exampleSet, ConcurrencyContext context) {
		int size = exampleSet.size();
		int width = exampleSet.getAttributes().allSize();
		String[] labels = new String[width];
		Column[] columns = new Column[width];
		List<Callable<Void>> futureColumns = new ArrayList<>();
		Map<String, List<ColumnMetaData>> meta = new HashMap<>();
		Attribute predictionAttribute = exampleSet.getAttributes().getPredictedLabel();
		int index = 0;
		for (Iterator<AttributeRole> allRoles = exampleSet.getAttributes().allAttributeRoles(); allRoles.hasNext(); ) {
			AttributeRole role = allRoles.next();

			Attribute attribute = role.getAttribute();
			labels[index] = attribute.getName();

			futureColumns.add(createDataRunnableAndStoreType(exampleSet, size, attribute, meta, columns, index));

			if (role.isSpecial()) {
				storeRole(role, attribute, meta, predictionAttribute);
			}
			index++;
		}
		restoreBeltMetaDataFromExampleSetUserData(exampleSet, meta, new HashSet<>(Arrays.asList(labels)));
		return buildTable(futureColumns, labels, columns, meta, context);
	}

	/**
	 * Converts the column specified by exampleSet and attribute into inside a runnable and stores it into the index entry of columns.
	 * Stores the ontology.
	 */
	private static Callable<Void> createDataRunnableAndStoreType(ExampleSet exampleSet, int size, Attribute attribute,
																 Map<String, List<ColumnMetaData>> meta, Column[] columns, int index) {
		switch (attribute.getValueType()) {
			case Ontology.NUMERICAL:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getRealColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.REAL:
				return () -> {
					columns[index] = getRealColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.INTEGER:
				return () -> {
					columns[index] = getIntegerColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.BINOMINAL:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getBinominalColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.NOMINAL:
				return () -> {
					columns[index] = getNominalColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.POLYNOMINAL:
			case Ontology.STRING:
			case Ontology.FILE_PATH:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getNominalColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.DATE:
				storeOntology(meta, attribute);
				return () -> {
					columns[index] = getDateColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.DATE_TIME:
				return () -> {
					columns[index] = getDateTimeColumn(exampleSet, size, attribute);
					return null;
				};
			case Ontology.TIME:
				return () -> {
					columns[index] = getTimeColumn(exampleSet, size, attribute);
					return null;
				};
			default:
				throw new UnsupportedOperationException(MESSAGE_UNKNOWN_TYPE);
		}
	}

	/**
	 * Same as {@link #createDataRunnableAndStoreType(ExampleSet, int, Attribute, Map, Column[], int)} but calculates
	 * the runnables directly.
	 */
	private static void createDataAndStoreType(ExampleSet exampleSet, int size,
											   Column[] columns, int index, Attribute attribute,
											   Map<String, List<ColumnMetaData>> meta) {
		switch (attribute.getValueType()) {
			case Ontology.NUMERICAL:
				storeOntology(meta, attribute);
				columns[index] = getRealColumn(exampleSet, size, attribute);
				break;
			case Ontology.REAL:
				columns[index] = getRealColumn(exampleSet, size, attribute);
				break;
			case Ontology.INTEGER:
				columns[index] = getIntegerColumn(exampleSet, size, attribute);
				break;
			case Ontology.BINOMINAL:
				columns[index] = getBinominalColumn(exampleSet, size, attribute);
				storeOntology(meta, attribute);
				break;
			case Ontology.NOMINAL:
				columns[index] = getNominalColumn(exampleSet, size, attribute);
				break;
			case Ontology.POLYNOMINAL:
			case Ontology.STRING:
			case Ontology.FILE_PATH:
				storeOntology(meta, attribute);
				columns[index] = getNominalColumn(exampleSet, size, attribute);
				break;
			case Ontology.DATE:
				storeOntology(meta, attribute);
				columns[index] = getDateColumn(exampleSet, size, attribute);
				break;
			case Ontology.DATE_TIME:
				columns[index] = getDateTimeColumn(exampleSet, size, attribute);
				break;
			case Ontology.TIME:
				columns[index] = getTimeColumn(exampleSet, size, attribute);
				break;
			default:
				throw new UnsupportedOperationException(MESSAGE_UNKNOWN_TYPE);
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
		int width = exampleSet.getAttributes().allSize();
		String[] labels = new String[width];
		Column[] columns = new Column[width];
		List<Callable<Void>> futureColumns = new ArrayList<>();
		Map<String, List<ColumnMetaData>> meta = new HashMap<>();
		ExampleTable table = getExampleTable(exampleSet);
		Attribute prediction = exampleSet.getAttributes().getPredictedLabel();
		int index = 0;
		for (Iterator<AttributeRole> allRoles = exampleSet.getAttributes().allAttributeRoles(); allRoles.hasNext(); ) {

			AttributeRole role = allRoles.next();
			Attribute attribute = role.getAttribute();
			labels[index] = attribute.getName();

			futureColumns.add(createTableCallablesAndStoreType(size, meta, table, attribute, columns, index));
			if (role.isSpecial()) {
				storeRole(role, attribute, meta, prediction);
			}
			index++;
		}
		restoreBeltMetaDataFromExampleSetUserData(exampleSet, meta, new HashSet<>(Arrays.asList(labels)));
		return buildTable(futureColumns, labels, columns, meta, context);
	}

	/**
	 * Stores the associated belt role and, if not all the info can be captured by the belt role, stores the original
	 * role name.
	 */
	private static void storeRole(AttributeRole role, Attribute attribute, Map<String, List<ColumnMetaData>> meta,
								  Attribute prediction) {
		String specialName = role.getSpecialName();
		ColumnRole beltRole = BeltConverter.convertRole(specialName);
		List<ColumnMetaData> columnMeta =
				meta.computeIfAbsent(attribute.getName(), s -> new ArrayList<>(2));
		columnMeta.add(beltRole);
		if (beltRole == ColumnRole.METADATA) {
			columnMeta.add(new LegacyRole(specialName));
		} else if (beltRole == ColumnRole.SCORE) {
			String predictionName = prediction == null ? null : prediction.getName();
			if (specialName.startsWith(CONFIDENCE_PREFIX)) {
				columnMeta.add(new ColumnReference(predictionName,
						specialName.substring(CONFIDENCE_PREFIX_LENGTH)));
			} else {
				columnMeta.add(new ColumnReference(predictionName));
				if (!Attributes.CONFIDENCE_NAME.equals(specialName)) {
					columnMeta.add(new LegacyRole(specialName));
				}
			}
		}
	}

	private static Column getNanosecondDateColumn(int size, ExampleTable table, Attribute attribute) {
		DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, true, false);
		for (int i = 0; i < table.size(); i++) {
			double value = table.getDataRow(i).get(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i, null);
			} else {
				// in Java 8 there is only floorDiv(long,long) but in Java 11 there is also floorDiv(long,int) - so
				// force longs to have code compiled with Java 11 and target Java 8 actually run with Java 8
				long longValue = (long) value;
				long longDivisor = MILLIS_PER_SECOND;
				buffer.set(i, Math.floorDiv(longValue, longDivisor),
						(int) (Math.floorMod(longValue, longDivisor) * NANOS_PER_MILLI_SECOND));
			}
		}
		return buffer.toColumn();
	}

	private static Column getTimeColumn(ExampleTable table, int size, Attribute attribute) {
		TimeBuffer buffer = Buffers.timeBuffer(size, false);
		Calendar calendar = Tools.getPreferredCalendar();
		for (int i = 0; i < table.size(); i++) {
			double value = table.getDataRow(i).get(attribute);
			if (Double.isNaN(value)) {
				buffer.set(i, null);
			} else {
				long nanos = BeltConverter.legacyTimeDoubleToNanoOfDay(value, calendar);
				buffer.set(i, nanos);
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
				buffer.set(i, ((long) value) / MILLIS_PER_SECOND);
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
		NumericBuffer intBuffer = Buffers.integer53BitBuffer(size, false);
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
		return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, packed, mapping, positiveIndex);
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
		return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, mapping);
	}

	/**
	 * Copies a nominal column from the example table using a nominal buffer.
	 */
	private static Column getBufferColumn(ExampleTable table, int size, Attribute attribute) {
		NominalBuffer nominalBuffer = BufferAccessor.get().newInt32Buffer(ColumnType.NOMINAL, size);
		NominalMapping mapping = attribute.getMapping();
		for (int i = 0; i < size; i++) {
			double value = table.getDataRow(i).get(attribute);
			if (Double.isNaN(value)) {
				nominalBuffer.set(i, null);
			} else {
				nominalBuffer.set(i, mapping.mapIndex((int) value));
			}
		}
		return nominalBuffer.toColumn();
	}

	/**
	 * Builds the table by running the future columns in the given context and creating a table from the results and
	 * the given labels.
	 */
	private static Table buildTable(List<Callable<Void>> columnConstructors, String[] labels, Column[] columns,
									Map<String, List<ColumnMetaData>> srcMeta, ConcurrencyContext context) {
		try {
			context.call(columnConstructors);
			return new Table(columns, labels, srcMeta);
		} catch (ExecutionException e) {
			return handleExecutionException(e);
		}
	}

	/**
	 * Handling of an {@link ExecutionException} to reuse at several places.
	 */
	static Table handleExecutionException(ExecutionException e) {
		Throwable cause = e.getCause();
		if (cause instanceof RuntimeException) {
			throw (RuntimeException) cause;
		} else if (cause instanceof Error) {
			throw (Error) cause;
		} else {
			throw new RuntimeException(cause.getMessage(), cause);
		}
	}

	/**
	 * Belt meta data (except for roles) cannot be stored in an ExampleSet. Therefore, we store the belt meta data in
	 * the ExampleSets's user data. This is the method that restores this saved belt meta data from the user data. Is
	 * adds the restored belt meta data to the given {@link TableBuilder}.
	 * <p>
	 * It is important that this method is called after the rest of the metadata has already been built because this
	 * method uses and updates the existing metadata.
	 *
	 * @param set
	 * 		the example set potentially holding some belt meta data in its user data
	 * @param builder
	 * 		the table builder that the belt meta data will be added to
	 */
	private static void restoreBeltMetaDataFromExampleSetUserData(ExampleSet set, TableBuilder builder,
																  Set<String> usedLabels) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, List<ColumnMetaData>> beltMetaData =
					(Map<String, List<ColumnMetaData>>) set.getUserData(IOOBJECT_USER_DATA_COLUMN_META_DATA_KEY);
			if (beltMetaData != null) {
				beltMetaData.forEach((label, columnMetaDataList) -> {
					if (usedLabels.contains(label)) {
						for (ColumnMetaData md : columnMetaDataList) {
							// column roles, legacy types and legacy roles are already determined by the example set.
							// for the rest we want to use the belt meta data that has been stored before
							if (!(md instanceof ColumnRole || md instanceof LegacyType || md instanceof LegacyRole)) {
								if (md instanceof ColumnReference) {
									// This is important for the special case that a column reference has been auto-
									// generated because of a confidence role in the example set. We override the auto-
									// generated column reference with the actual one.
									builder.removeMetaData(label, ColumnReference.class);
								}
								builder.addMetaData(label, md);
							}
						}
					}
				});
			}
		} catch (ClassCastException e) {
			// well then there is nothing we can do
		}
	}

	/**
	 * Belt meta data (except for roles) cannot be stored in an ExampleSet. Therefore, we store the belt meta data in
	 * the ExampleSets's user data. This is the method that restores this saved belt meta data from the user data. Is
	 * adds the restored belt meta data to the given map.
	 * <p>
	 * It is important that this method is called after the rest of the metadata has already been built because this
	 * method used and updates the existing metadata.
	 *
	 * @param set
	 * 		the example set potentially holding some belt meta data in its user data
	 * @param incompleteMetaData
	 * 		the belt meta data will be added to this map
	 */
	private static void restoreBeltMetaDataFromExampleSetUserData(ExampleSet set, Map<String,
			List<ColumnMetaData>> incompleteMetaData, Set<String> usedLabels) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, List<ColumnMetaData>> beltMetaData =
					(Map<String, List<ColumnMetaData>>) set.getUserData(IOOBJECT_USER_DATA_COLUMN_META_DATA_KEY);
			if (beltMetaData != null) {
				beltMetaData.forEach((label, columnMetaDataList) -> {
					if (usedLabels.contains(label)) {
						incompleteMetaData.computeIfAbsent(label, k -> new ArrayList<>());
						List<ColumnMetaData> metaDataForLabel = incompleteMetaData.get(label);
						for (ColumnMetaData md : columnMetaDataList) {
							// column roles, legacy types and legacy roles are already determined by the example set.
							// for the rest we want to use the belt meta data that has been stored before
							if (!(md instanceof ColumnRole || md instanceof LegacyType || md instanceof LegacyRole)) {
								if (md instanceof ColumnReference) {
									// This is important for the special case that a column reference has been auto-
									// generated because of a confidence role in the example set. We override the auto-
									// generated column reference with the actual one.
									metaDataForLabel.removeIf(x -> x instanceof ColumnReference);
								}
								metaDataForLabel.add(md);
							}
						}
					}
				});
			}
		} catch (ClassCastException e) {
			// well then there is nothing we can do
		}
	}

}
