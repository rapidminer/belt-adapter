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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.stream.StreamSupport;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.RapidMiner;
import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.util.ColumnAnnotation;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnReference;
import com.rapidminer.belt.util.ColumnRole;
import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.core.concurrency.ExecutionStoppedException;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.AttributeTransformation;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.example.set.Condition;
import com.rapidminer.example.set.ConditionedExampleSet;
import com.rapidminer.example.set.HeaderExampleSet;
import com.rapidminer.example.set.MappedExampleSet;
import com.rapidminer.example.set.Partition;
import com.rapidminer.example.set.RemappedExampleSet;
import com.rapidminer.example.set.SimplePartitionBuilder;
import com.rapidminer.example.set.SortedExampleSet;
import com.rapidminer.example.set.SplittedExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.NominalMapping;
import com.rapidminer.example.utils.ExampleSetBuilder;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.Annotations;
import com.rapidminer.operator.tools.ExpressionEvaluationException;
import com.rapidminer.studio.concurrency.internal.SequentialConcurrencyContext;
import com.rapidminer.test.asserter.AsserterFactoryRapidMiner;
import com.rapidminer.test_utils.RapidAssert;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.ParameterService;
import com.rapidminer.tools.Tools;


/**
 * Tests the {@link com.rapidminer.belt.table.BeltConverter}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class BeltConverterTest {

	/**
	 * Meta data used for testing.
	 */
	private static class TestMetaData implements ColumnMetaData {

		private final String someParameter;

		public TestMetaData(String someParameter) {
			this.someParameter = someParameter;
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (other == null || getClass() != other.getClass()) {
				return false;
			}
			TestMetaData that = (TestMetaData) other;
			return Objects.equals(someParameter, that.someParameter);
		}

		@Override
		public String type() {
			return "com.rapidminer.belt.meta.column.testmetadata";
		}

		@Override
		public Uniqueness uniqueness() {
			return Uniqueness.NONE;
		}
	}

	private static final ConcurrencyContext CONTEXT = new ConcurrencyContext() {

		private ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

		@Override
		public <T> List<Future<T>> submit(List<Callable<T>> callables) throws IllegalArgumentException {
			List<Future<T>> futures = new ArrayList<>();
			for (Callable<T> callable : callables) {
				futures.add(pool.submit(callable));
			}
			return futures;
		}

		@Override
		public <T> List<T> call(List<Callable<T>> callables)
				throws ExecutionException, ExecutionStoppedException, IllegalArgumentException {
			List<Future<T>> futures = submit(callables);
			List<T> results = new ArrayList<>();
			for (Future<T> future : futures) {
				try {
					results.add(future.get());
				} catch (InterruptedException e) {
					throw new RuntimeException("must not happen");
				}
			}
			return results;
		}

		@Override
		public void run(List<Runnable> runnables)
				throws ExecutionException, ExecutionStoppedException, IllegalArgumentException {
		}

		@Override
		public <T> List<T> invokeAll(List<ForkJoinTask<T>> tasks)
				throws ExecutionException, ExecutionStoppedException, IllegalArgumentException {
			return null;
		}

		@Override
		public <T> T invoke(ForkJoinTask<T> task)
				throws ExecutionException, ExecutionStoppedException, IllegalArgumentException {
			return null;
		}

		@Override
		public int getParallelism() {
			return pool.getParallelism();
		}

		@Override
		public <T> List<T> collectResults(List<Future<T>> futures)
				throws ExecutionException, ExecutionStoppedException, IllegalArgumentException {
			return null;
		}

		@Override
		public void checkStatus() throws ExecutionStoppedException {
		}

	};

	private static double[] readColumnToArray(Table table, int column) {
		double[] data = new double[table.height()];
		NumericReader reader = Readers.numericReader(table.column(column));
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	private static String[] readColumnToStringArray(Table table, int column) {
		String[] data = new String[table.height()];
		Column col = table.column(column);
		List<String> categoricalMapping = ColumnAccessor.get().getDictionaryList(col.getDictionary());
		CategoricalReader reader = Readers.categoricalReader(col);
		for (int j = 0; j < table.height(); j++) {
			data[j] = categoricalMapping.get(reader.read());
		}
		return data;
	}

	static double[][] readTableToArray(Table table) {
		double[][] result = new double[table.width()][];
		Arrays.setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	private static String[][] readTableToStringArray(Table table) {
		String[][] result = new String[table.width()][];
		Arrays.setAll(result, i -> readColumnToStringArray(table, i));
		return result;
	}

	static double[][] readExampleSetToArray(ExampleSet set) {
		double[][] result = new double[set.getAttributes().size()][];
		int i = 0;
		for (Attribute att : set.getAttributes()) {
			result[i] = new double[set.size()];
			int j = 0;
			for (Example example : set) {
				result[i][j] = example.getValue(att);
				j++;
			}
			i++;
		}
		return result;
	}

	private static String[][] readExampleSetToStringArray(ExampleSet set) {
		String[][] result = new String[set.getAttributes().size()][];
		int i = 0;
		for (Attribute att : set.getAttributes()) {
			result[i] = new String[set.size()];
			int j = 0;
			for (Example example : set) {
				double value = example.getValue(att);
				if (Double.isNaN(value)) {
					result[i][j] = null;
				} else {
					result[i][j] = att.getMapping().mapIndex((int) value);
				}
				j++;
			}
			i++;
		}
		return result;
	}

	public static class InputValidation {

		@Test(expected = IllegalArgumentException.class)
		public void testSetToTableNullSet() {
			com.rapidminer.belt.table.BeltConverter.convert((ExampleSet) null, CONTEXT);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetToTableNullContext() {
			com.rapidminer.belt.table.BeltConverter.convert(ExampleSetFactory.createExampleSet(new double[][]{new double[]{0}}), null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTableToSetNullTable() {
			com.rapidminer.belt.table.BeltConverter.convert((IOTable) null, CONTEXT);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTableToSetSequentiallyNullTable() {
			com.rapidminer.belt.table.BeltConverter.convertSequentially((IOTable) null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTableToSetNullContext() {
			com.rapidminer.belt.table.BeltConverter.convert(new IOTable(Builders.newTableBuilder(1).build(Belt.defaultContext())), null);
		}
	}

	@RunWith(Parameterized.class)
	public static class SetToTable {

		public SetToTable(boolean legacyMode) {
			ParameterService.setParameterValue(RapidMiner.PROPERTY_RAPIDMINER_SYSTEM_LEGACY_DATA_MGMT,
					String.valueOf(legacyMode));
		}

		@Parameters(name = "legacyMode={0}")
		public static Collection<Object> params() {
			return Arrays.asList(true, false);
		}

		@Test
		public void testSimple() {
			Attribute attribute1 = attributeInt();
			Attribute attribute2 = attributeReal();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i + 1).withColumnFiller(attribute2, i -> i + 1.7).build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			double[][] result = readTableToArray(table);
			double[][] expected = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominal() {
			Attribute attribute1 = attributeDogCatMouse();
			Attribute attribute2 = attributeYesNo();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i % 3).withColumnFiller(attribute2, i -> i % 2).build();
			set.getExample(10).setValue(attribute1, Double.NaN);
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			String[][] result = readTableToStringArray(table);
			String[][] expected = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testBinominal() {
			Attribute attribute1 = AttributeFactory.createAttribute("oneNegative", Ontology.BINOMINAL);
			attribute1.getMapping().mapString("one");
			assertEquals("one", attribute1.getMapping().getNegativeString());
			assertNull(attribute1.getMapping().getPositiveString());

			Attribute attribute2 = AttributeFactory.createAttribute("empty", Ontology.BINOMINAL);
			assertNull(attribute2.getMapping().getPositiveString());
			assertNull(attribute2.getMapping().getNegativeString());

			Attribute attribute3 = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			attribute3.getMapping().mapString("negative");
			attribute3.getMapping().mapString("positive");
			assertEquals("negative", attribute3.getMapping().getNegativeString());
			assertEquals("positive", attribute3.getMapping().getPositiveString());

			ExampleSet set = ExampleSets.from(attribute1, attribute2, attribute3).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i % 2 == 0 ? Double.NaN : 0).withColumnFiller(attribute2,
							i -> Double.NaN).withColumnFiller(attribute3, i -> i % 2 == 0 ? Double.NaN : 1).build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			String[][] result = readTableToStringArray(table);
			String[][] expected = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);

			Dictionary oneNegative = table.column("oneNegative").getDictionary();
			assertTrue(oneNegative.isBoolean());
			assertFalse(oneNegative.hasPositive());
			assertEquals(attribute1.getMapping().getNegativeString(), oneNegative.get(oneNegative.getNegativeIndex()));
			assertEquals(1, oneNegative.size());

			Dictionary empty = table.column("empty").getDictionary();
			assertTrue(empty.isBoolean());
			assertFalse(empty.hasPositive());
			assertFalse(empty.hasNegative());
			assertEquals(0, empty.size());

			Dictionary binominal = table.column("binominal").getDictionary();
			assertTrue(binominal.isBoolean());
			assertEquals(2, binominal.size());
			assertEquals(attribute3.getMapping().getNegativeString(), binominal.get(binominal.getNegativeIndex()));
			assertEquals(attribute3.getMapping().getPositiveString(), binominal.get(binominal.getPositiveIndex()));
		}

		@Test
		public void testNominalUnusedValue() {
			Attribute attribute1 = attributeDogCatMouse();
			Attribute attribute2 = attributeYesNo();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i % 2).withColumnFiller(attribute2, i -> 1).build();
			set.getExample(10).setValue(attribute1, Double.NaN);
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			String[][] result = readTableToStringArray(table);
			String[][] expected = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominalDoubleValue() {
			Attribute attribute1 = attributeDogCatMouse();
			Attribute attribute2 = attributeYesNo();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i % 3).withColumnFiller(attribute2, i -> i % 2).build();
			set.getExample(10).setValue(attribute1, Double.NaN);
			attribute1.getMapping().setMapping("cat", 0);
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			String[][] result = readTableToStringArray(table);
			String[][] expected = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominalNullValue() {
			Attribute attribute1 = attributeDogCatMouse();
			Attribute attribute2 = attributeYesNo();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i % 3).withColumnFiller(attribute2, i -> i % 2).build();
			set.getExample(10).setValue(attribute1, Double.NaN);
			attribute1.getMapping().setMapping(null, 2);
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			String[][] result = readTableToStringArray(table);
			String[][] expected = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testManyColumns() {
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 0; i < 60; i++) {
				attributes.add(attributeReal(i));
			}
			ExampleSetBuilder builder = ExampleSets.from(attributes).withBlankSize(20);
			for (int i = 0; i < 60; i++) {
				builder.withColumnFiller(attributes.get(i), j -> j + 1.7);
			}
			ExampleSet set = builder.build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			double[][] result = readTableToArray(table);
			double[][] expected = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testUnsafeAttribute() {
			Attribute attribute1 = attributeInt();
			Attribute attribute2 = attributeReal();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i + 1).withColumnFiller(attribute2, i -> i + 1.7).build();
			set.getAttributes().allAttributes().next().addTransformation(new AttributeTransformation() {
				@Override
				public double transform(Attribute attribute, double value) {
					return value;
				}

				@Override
				public double inverseTransform(Attribute attribute, double value) {
					return value;
				}

				@Override
				public boolean isReversable() {
					return false;
				}

				@Override
				public Object clone() {
					return null;
				}
			});
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			double[][] result = readTableToArray(table);
			double[][] expected = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}


		@Test
		public void testTypes() {
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 1; i < Ontology.VALUE_TYPE_NAMES.length; i++) {
				attributes.add(AttributeFactory.createAttribute(i));
			}
			ExampleSet set = ExampleSets.from(attributes)
					.build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			Column.TypeId[] result =
					table.labels().stream().map(label -> table.column(label).type().id()).toArray(Column
							.TypeId[]::new);
			Column.TypeId[] expected =
					new Column.TypeId[]{Column.TypeId.NOMINAL, Column.TypeId.REAL, Column.TypeId.INTEGER_53_BIT,
							Column.TypeId.REAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL,
							Column.TypeId.NOMINAL, Column.TypeId.DATE_TIME, Column.TypeId.DATE_TIME,
							Column.TypeId.TIME};
			assertArrayEquals(expected, result);

			com.rapidminer.belt.table.LegacyType[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, com.rapidminer.belt.table.LegacyType.class))
					.toArray(com.rapidminer.belt.table.LegacyType[]::new);
			com.rapidminer.belt.table.LegacyType[] legacyExpected =
					new com.rapidminer.belt.table.LegacyType[]{null,
							com.rapidminer.belt.table.LegacyType.NUMERICAL, null, null,
							com.rapidminer.belt.table.LegacyType.STRING,
							com.rapidminer.belt.table.LegacyType.BINOMINAL, com.rapidminer.belt.table.LegacyType.POLYNOMINAL,
							com.rapidminer.belt.table.LegacyType.FILE_PATH, null,
							com.rapidminer.belt.table.LegacyType.DATE, null};
			assertArrayEquals(legacyExpected, legacyResult);
		}

		@Test
		public void testTypesView() {
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 1; i < Ontology.VALUE_TYPE_NAMES.length; i++) {
				attributes.add(AttributeFactory.createAttribute(i));
			}
			ExampleSet set = new SortedExampleSet(ExampleSets.from(attributes)
					.build(), attributes.get(0), SortedExampleSet.INCREASING);
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			Column.TypeId[] result =
					table.labels().stream().map(label -> table.column(label).type().id()).toArray(Column
							.TypeId[]::new);
			Column.TypeId[] expected =
					new Column.TypeId[]{Column.TypeId.NOMINAL, Column.TypeId.REAL, Column.TypeId.INTEGER_53_BIT,
							Column.TypeId.REAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL,
							Column.TypeId.NOMINAL, Column.TypeId.DATE_TIME, Column.TypeId.DATE_TIME,
							Column.TypeId.TIME};
			assertArrayEquals(expected, result);

			com.rapidminer.belt.table.LegacyType[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, com.rapidminer.belt.table.LegacyType.class))
					.toArray(com.rapidminer.belt.table.LegacyType[]::new);
			com.rapidminer.belt.table.LegacyType[] legacyExpected =
					new com.rapidminer.belt.table.LegacyType[]{null,
							com.rapidminer.belt.table.LegacyType.NUMERICAL, null, null,
							com.rapidminer.belt.table.LegacyType.STRING,
							com.rapidminer.belt.table.LegacyType.BINOMINAL, com.rapidminer.belt.table.LegacyType.POLYNOMINAL,
							com.rapidminer.belt.table.LegacyType.FILE_PATH, null,
							com.rapidminer.belt.table.LegacyType.DATE, null};
			assertArrayEquals(legacyExpected, legacyResult);
		}


		@Test
		public void testRoles() {
			String[] roles = new String[]{Attributes.ID_NAME, Attributes.CONFIDENCE_NAME + "_" + "Yes",
					Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
					BeltConverter.INTERPRETATION_NAME, BeltConverter.ENCODING_NAME, BeltConverter.SOURCE_NAME,
					Attributes.CLUSTER_NAME, Attributes.WEIGHT_NAME, Attributes.BATCH_NAME, Attributes.OUTLIER_NAME,
					Attributes.CONFIDENCE_NAME,
					Attributes.CLASSIFICATION_COST, "ignore-me", "confidence(yes)", "cluster_1_probability"};
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 0; i < roles.length + 1; i++) {
				attributes.add(AttributeFactory.createAttribute(Ontology.NUMERICAL));
			}
			ExampleSetBuilder builder = ExampleSets.from(attributes);
			for (int i = 1; i < roles.length + 1; i++) {
				builder.withRole(attributes.get(i), roles[i - 1]);
			}
			ExampleSet set = builder.build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ColumnRole[] result = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new);
			ColumnRole[] expected =
					new ColumnRole[]{null, ColumnRole.ID, ColumnRole.SCORE, ColumnRole.LABEL, ColumnRole.PREDICTION,
							ColumnRole.INTERPRETATION, ColumnRole.ENCODING, ColumnRole.SOURCE,
							ColumnRole.CLUSTER,
							ColumnRole.WEIGHT, ColumnRole.BATCH, ColumnRole.OUTLIER, ColumnRole
							.SCORE,	ColumnRole.METADATA, ColumnRole.METADATA, ColumnRole.SCORE, ColumnRole.METADATA};
			assertArrayEquals(expected, result);

			com.rapidminer.belt.table.LegacyRole[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, com.rapidminer.belt.table.LegacyRole.class))
					.toArray(com.rapidminer.belt.table.LegacyRole[]::new);
			com.rapidminer.belt.table.LegacyRole[] legacyExpected =
					new com.rapidminer.belt.table.LegacyRole[]{null, null, null, null, null, null, null, null, null,
							null, null, null, null,
							new LegacyRole(Attributes.CLASSIFICATION_COST),
							new LegacyRole("ignore-me"), new LegacyRole("confidence(yes)"), new LegacyRole("cluster_1_probability")};
			assertArrayEquals(legacyExpected, legacyResult);

			ColumnReference[] references = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, ColumnReference.class))
					.toArray(ColumnReference[]::new);
			ColumnReference[] referencesExpected =
					new ColumnReference[]{null, null,
							new ColumnReference(set.getAttributes().getPredictedLabel().getName(), "Yes"), null,
							null, null, null, null,
							null, null, null, null, new ColumnReference(set.getAttributes().getPredictedLabel().getName()),
							null, null, new ColumnReference(set.getAttributes().getPredictedLabel().getName()), null};
			assertArrayEquals(referencesExpected, references);
		}

		@Test
		public void testDuplicateRoles() {
			ColumnRole[] roles = ColumnRole.values();
			// build expected example set
			List<Attribute> attributes = new ArrayList<>();
			for (ColumnRole role : roles) {
				String name1 = role.name() + "-1", name2 = role.name() + "-2", name3 = role.name() + "-3";
				attributes.add(AttributeFactory.createAttribute(name1, Ontology.REAL));
				attributes.add(AttributeFactory.createAttribute(name2, Ontology.REAL));
				attributes.add(AttributeFactory.createAttribute(name3, Ontology.REAL));
			}
			ExampleSetBuilder exampleSetBuilder = ExampleSets.from(attributes);
			int attributeIndex = 0;
			for (ColumnRole role : roles) {
				String baseName;
				if (role == ColumnRole.SCORE) {
					baseName = Attributes.CONFIDENCE_NAME;
				} else {
					baseName = role.name().toLowerCase(Locale.ROOT);
				}
				exampleSetBuilder.withRole(attributes.get(attributeIndex++), baseName);
				exampleSetBuilder.withRole(attributes.get(attributeIndex++), baseName + "_2");
				exampleSetBuilder.withRole(attributes.get(attributeIndex++), baseName + "_3");
			}
			ExampleSet expectedExampleSet = exampleSetBuilder.build();

			// build expected table
			TableBuilder tableBuilder = new TableBuilder(0);
			for (ColumnRole role : roles) {
				String name1 = role.name() + "-1", name2 = role.name() + "-2", name3 = role.name() + "-3";
				tableBuilder.addReal(name1, i -> Math.random());
				tableBuilder.addMetaData(name1, role);
				tableBuilder.addReal(name2, i -> Math.random());
				tableBuilder.addMetaData(name2, role);
				tableBuilder.addReal(name3, i -> Math.random());
				tableBuilder.addMetaData(name3, role);
			}
			Table expectedTable = tableBuilder.build(Belt.defaultContext());

			// convert example set / table
			ExampleSet convertedExampleSet = BeltConverter.convert(new IOTable(expectedTable), CONTEXT);
			Table convertedTable = BeltConverter.convert(expectedExampleSet, CONTEXT).getTable();

			// check if results are equal
			for (ColumnRole role : roles) {
				String name1 = role.name() + "-1", name2 = role.name() + "-2", name3 = role.name() + "-3";
				assertEquals(expectedExampleSet.getAttributes().getRole(name1).getSpecialName(),
						convertedExampleSet.getAttributes().getRole(name1).getSpecialName());
				assertEquals(expectedExampleSet.getAttributes().getRole(name2).getSpecialName(),
						convertedExampleSet.getAttributes().getRole(name2).getSpecialName());
				assertEquals(expectedExampleSet.getAttributes().getRole(name3).getSpecialName(),
						convertedExampleSet.getAttributes().getRole(name3).getSpecialName());
			}
			assertArrayEquals(expectedTable.labels().stream()
					.map(label -> expectedTable.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new), convertedTable.labels().stream()
					.map(label -> convertedTable.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new));

			// and convert them again to make sure
			Table doubleConvertedTable = BeltConverter.convert(convertedExampleSet, CONTEXT).getTable();
			ExampleSet doubleConvertedExampleSet = BeltConverter.convert(new IOTable(convertedTable), CONTEXT);

			for (ColumnRole role : roles) {
				String name1 = role.name() + "-1", name2 = role.name() + "-2", name3 = role.name() + "-3";
				assertEquals(expectedExampleSet.getAttributes().getRole(name1).getSpecialName(),
						doubleConvertedExampleSet.getAttributes().getRole(name1).getSpecialName());
				assertEquals(expectedExampleSet.getAttributes().getRole(name2).getSpecialName(),
						doubleConvertedExampleSet.getAttributes().getRole(name2).getSpecialName());
				assertEquals(expectedExampleSet.getAttributes().getRole(name3).getSpecialName(),
						doubleConvertedExampleSet.getAttributes().getRole(name3).getSpecialName());
			}
			assertArrayEquals(expectedTable.labels().stream()
					.map(label -> expectedTable.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new), doubleConvertedTable.labels().stream()
					.map(label -> doubleConvertedTable.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new));
		}

		/**
		 * Some of the belt meta data has no equivalent in the example set representation. Therefore, we store the belt
		 * meta data in the ExampleSet's user data. This test checks if this is done correctly.
		 */
		@Test
		public void testPreserveBeltMetaData() {
			// BUILD ORIGINAL TABLE
			TableBuilder tableBuilder = new TableBuilder(0);

			// add a prediction and two corresponding score columns
			tableBuilder.addNominal("PredictionColumnOne", i -> i % 2 == 0 ? "YES" : "NO");
			tableBuilder.addReal("ScoreColumnYES", i -> Math.random());
			tableBuilder.addReal("ScoreColumnNO", i -> Math.random());
			tableBuilder.addMetaData("ScoreColumnYES", ColumnRole.SCORE);
			tableBuilder.addMetaData("ScoreColumnNO", ColumnRole.SCORE);
			tableBuilder.addMetaData("PredictionColumnOne", ColumnRole.PREDICTION);
			tableBuilder.addMetaData("PredictionColumnOne", new ColumnAnnotation("This is my wonderful prediction!"));
			tableBuilder.addMetaData("ScoreColumnYES", new ColumnReference("PredictionColumnOne", "YES"));
			tableBuilder.addMetaData("ScoreColumnNO", new ColumnReference("PredictionColumnOne", "NO"));

			// add another prediction and two corresponding score columns
			tableBuilder.addNominal("PredictionColumnTwo", i -> i % 2 == 0 ? "play" : "work");
			tableBuilder.addReal("ScoreColumnPlay", i -> Math.random());
			tableBuilder.addReal("ScoreColumnWork", i -> Math.random());
			tableBuilder.addMetaData("ScoreColumnPlay", ColumnRole.SCORE);
			tableBuilder.addMetaData("ScoreColumnWork", ColumnRole.SCORE);
			tableBuilder.addMetaData("PredictionColumnTwo", ColumnRole.PREDICTION);
			tableBuilder.addMetaData("PredictionColumnTwo", new TestMetaData("A first parameter"));
			tableBuilder.addMetaData("PredictionColumnTwo", new TestMetaData("A second parameter"));
			tableBuilder.addMetaData("PredictionColumnTwo", new TestMetaData("A third parameter"));
			tableBuilder.addMetaData("ScoreColumnWork", new ColumnReference("PredictionColumnTwo", "work"));
			tableBuilder.addMetaData("ScoreColumnPlay", new ColumnReference("PredictionColumnTwo", "play"));

			Table originalTable = tableBuilder.build(Belt.defaultContext());

			// CHECK CONVERTED EXAMPLE SET
			ExampleSet convertedExampleSet = BeltConverter.convert(new IOTable(originalTable), CONTEXT);
			assertEquals("confidence_YES", convertedExampleSet.getAttributes().findRoleByName("ScoreColumnYES").getSpecialName());
			assertEquals("confidence_NO", convertedExampleSet.getAttributes().findRoleByName("ScoreColumnNO").getSpecialName());
			assertEquals("confidence_work", convertedExampleSet.getAttributes().findRoleByName("ScoreColumnWork").getSpecialName());
			assertEquals("confidence_play", convertedExampleSet.getAttributes().findRoleByName("ScoreColumnPlay").getSpecialName());
			String predictionOneRoleName = convertedExampleSet.getAttributes().findRoleByName("PredictionColumnOne").getSpecialName();
			String predictionTwoRoleName = convertedExampleSet.getAttributes().findRoleByName("PredictionColumnTwo").getSpecialName();
			String predictedLabel = convertedExampleSet.getAttributes().getPredictedLabel().getName();
			assertTrue(predictionOneRoleName.equals("prediction") && predictionTwoRoleName.equals("prediction_2")
					|| predictionTwoRoleName.equals("prediction") && predictionOneRoleName.equals("prediction_2"));
			assertTrue(predictedLabel.equals("PredictionColumnOne") || predictedLabel.equals("PredictionColumnTwo"));
			assertEquals(6, convertedExampleSet.getAttributes().specialSize());

			// CHECK DOUBLE-CONVERTED TABLE
			Table convertedTable = BeltConverter.convert(convertedExampleSet, CONTEXT).getTable();
			assertEquals(2, convertedTable.getMetaData().get("PredictionColumnOne").size());
			assertEquals(4, convertedTable.getMetaData().get("PredictionColumnTwo").size());
			assertEquals(2, convertedTable.getMetaData().get("ScoreColumnYES").size());
			assertEquals(2, convertedTable.getMetaData().get("ScoreColumnNO").size());
			assertEquals(2, convertedTable.getMetaData().get("ScoreColumnPlay").size());
			assertEquals(2, convertedTable.getMetaData().get("ScoreColumnWork").size());
			assertEquals(6, convertedTable.width());

			assertEquals(ColumnRole.PREDICTION, convertedTable.getFirstMetaData("PredictionColumnOne", ColumnRole.class));
			assertEquals(ColumnRole.PREDICTION, convertedTable.getFirstMetaData("PredictionColumnTwo", ColumnRole.class));
			assertEquals(ColumnRole.SCORE, convertedTable.getFirstMetaData("ScoreColumnYES", ColumnRole.class));
			assertEquals(ColumnRole.SCORE, convertedTable.getFirstMetaData("ScoreColumnNO", ColumnRole.class));
			assertEquals(ColumnRole.SCORE, convertedTable.getFirstMetaData("ScoreColumnPlay", ColumnRole.class));
			assertEquals(ColumnRole.SCORE, convertedTable.getFirstMetaData("ScoreColumnWork", ColumnRole.class));

			assertEquals(new ColumnReference("PredictionColumnOne", "YES"),
					convertedTable.getFirstMetaData("ScoreColumnYES", ColumnReference.class));
			assertEquals(new ColumnReference("PredictionColumnOne", "NO"),
					convertedTable.getFirstMetaData("ScoreColumnNO", ColumnReference.class));
			assertEquals(new ColumnReference("PredictionColumnTwo", "play"),
					convertedTable.getFirstMetaData("ScoreColumnPlay", ColumnReference.class));
			assertEquals(new ColumnReference("PredictionColumnTwo", "work"),
					convertedTable.getFirstMetaData("ScoreColumnWork", ColumnReference.class));

			assertEquals(new ColumnAnnotation("This is my wonderful prediction!"),
					convertedTable.getFirstMetaData("PredictionColumnOne", ColumnAnnotation.class));

			List<TestMetaData> testMetaData = convertedTable.getMetaData("PredictionColumnTwo", TestMetaData.class);
			assertEquals(3, testMetaData.size());
			assertTrue(testMetaData.contains(new TestMetaData("A first parameter")));
			assertTrue(testMetaData.contains(new TestMetaData("A second parameter")));
			assertTrue(testMetaData.contains(new TestMetaData("A third parameter")));
		}

		/**
		 * A belt table is converted to example set, an attribute is removed / renamed and then the example set is
		 * converted back. This test checks that no exception is thrown.
		 */
		@Test
		public void testRemoveAndRenameAttribute() {
			// BUILD ORIGINAL TABLE
			TableBuilder tableBuilder = new TableBuilder(0);

			// add a prediction and two corresponding score columns
			tableBuilder.addNominal("PredictionColumnOne", i -> i % 2 == 0 ? "YES" : "NO");
			tableBuilder.addReal("ScoreColumnYES", i -> Math.random());
			tableBuilder.addReal("ScoreColumnNO", i -> Math.random());
			tableBuilder.addMetaData("ScoreColumnYES", ColumnRole.SCORE);
			tableBuilder.addMetaData("ScoreColumnNO", ColumnRole.SCORE);
			tableBuilder.addMetaData("PredictionColumnOne", ColumnRole.PREDICTION);
			tableBuilder.addMetaData("PredictionColumnOne", new ColumnAnnotation("This is my wonderful prediction!"));
			tableBuilder.addMetaData("ScoreColumnYES", new ColumnReference("PredictionColumnOne", "YES"));
			tableBuilder.addMetaData("ScoreColumnNO", new ColumnReference("PredictionColumnOne", "NO"));

			// add another prediction and two corresponding score columns
			tableBuilder.addNominal("PredictionColumnTwo", i -> i % 2 == 0 ? "play" : "work");
			tableBuilder.addReal("ScoreColumnPlay", i -> Math.random());
			tableBuilder.addReal("ScoreColumnWork", i -> Math.random());
			tableBuilder.addMetaData("ScoreColumnPlay", ColumnRole.SCORE);
			tableBuilder.addMetaData("ScoreColumnWork", ColumnRole.SCORE);
			tableBuilder.addMetaData("PredictionColumnTwo", ColumnRole.PREDICTION);
			tableBuilder.addMetaData("PredictionColumnTwo", new TestMetaData("A first parameter"));
			tableBuilder.addMetaData("ScoreColumnWork", new ColumnReference("PredictionColumnTwo", "work"));
			tableBuilder.addMetaData("ScoreColumnPlay", new ColumnReference("PredictionColumnTwo", "play"));

			Table originalTable = tableBuilder.build(Belt.defaultContext());

			// MODIFY CONVERTED EXAMPLE SET
			ExampleSet convertedExampleSet = BeltConverter.convert(new IOTable(originalTable), CONTEXT);
			Attributes attributes = convertedExampleSet.getAttributes();
			attributes.remove(attributes.get("PredictionColumnOne"));
			attributes.remove(attributes.get("ScoreColumnWork"));
			attributes.get("PredictionColumnTwo").setName("NewName");
			assertEquals(4, attributes.specialSize());
			assertTrue(attributes.contains(attributes.get("NewName")));

			// CHECK DOUBLE-CONVERTED TABLE
			Table convertedTable = BeltConverter.convert(convertedExampleSet, CONTEXT).getTable();
			assertFalse(convertedTable.contains("PredictionColumnOne"));
			assertFalse(convertedTable.contains("ScoreColumnWork"));
			assertFalse(convertedTable.contains("PredictionColumnTwo"));
			assertEquals(1, convertedTable.getMetaData().get("NewName").size());
			assertEquals(2, convertedTable.getMetaData().get("ScoreColumnYES").size());
			assertEquals(2, convertedTable.getMetaData().get("ScoreColumnNO").size());
			assertEquals(2, convertedTable.getMetaData().get("ScoreColumnPlay").size());
			assertEquals(4, convertedTable.width());

			assertEquals(ColumnRole.PREDICTION, convertedTable.getFirstMetaData("NewName", ColumnRole.class));
			assertEquals(ColumnRole.SCORE, convertedTable.getFirstMetaData("ScoreColumnYES", ColumnRole.class));
			assertEquals(ColumnRole.SCORE, convertedTable.getFirstMetaData("ScoreColumnNO", ColumnRole.class));
			assertEquals(ColumnRole.SCORE, convertedTable.getFirstMetaData("ScoreColumnPlay", ColumnRole.class));

			assertEquals(new ColumnReference("PredictionColumnOne", "YES"),
					convertedTable.getFirstMetaData("ScoreColumnYES", ColumnReference.class));
			assertEquals(new ColumnReference("PredictionColumnOne", "NO"),
					convertedTable.getFirstMetaData("ScoreColumnNO", ColumnReference.class));
			assertEquals(new ColumnReference("PredictionColumnTwo", "play"),
					convertedTable.getFirstMetaData("ScoreColumnPlay", ColumnReference.class));
		}

		@Test
		public void testRolesView() {
			String[] roles = new String[]{Attributes.ID_NAME, Attributes.CONFIDENCE_NAME + "_" + "Yes",
					Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
					BeltConverter.INTERPRETATION_NAME, BeltConverter.ENCODING_NAME, BeltConverter.SOURCE_NAME,
					Attributes.CLUSTER_NAME, Attributes.WEIGHT_NAME, Attributes.BATCH_NAME, Attributes.OUTLIER_NAME,
					Attributes.CONFIDENCE_NAME,
					Attributes.CLASSIFICATION_COST, "ignore-me"};
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 0; i < roles.length + 1; i++) {
				attributes.add(AttributeFactory.createAttribute(Ontology.NUMERICAL));
			}
			ExampleSetBuilder builder = ExampleSets.from(attributes);
			for (int i = 1; i < roles.length + 1; i++) {
				builder.withRole(attributes.get(i), roles[i - 1]);
			}
			ExampleSet set = new SortedExampleSet(builder.build(), attributes.get(1), SortedExampleSet.DECREASING);
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ColumnRole[] result = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new);
			ColumnRole[] expected =
					new ColumnRole[]{null, ColumnRole.ID, ColumnRole.SCORE, ColumnRole.LABEL, ColumnRole.PREDICTION,
							ColumnRole.INTERPRETATION, ColumnRole.ENCODING, ColumnRole.SOURCE,
							ColumnRole.CLUSTER,
							ColumnRole.WEIGHT, ColumnRole.BATCH, ColumnRole.OUTLIER, ColumnRole.SCORE,
							ColumnRole.METADATA, ColumnRole.METADATA};
			assertArrayEquals(expected, result);

			com.rapidminer.belt.table.LegacyRole[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, com.rapidminer.belt.table.LegacyRole.class))
					.toArray(com.rapidminer.belt.table.LegacyRole[]::new);
			com.rapidminer.belt.table.LegacyRole[] legacyExpected =
					new com.rapidminer.belt.table.LegacyRole[]{null, null, null, null, null, null, null, null, null,
							null, null, null, null,
							new com.rapidminer.belt.table.LegacyRole(Attributes.CLASSIFICATION_COST),
							new com.rapidminer.belt.table.LegacyRole("ignore-me")};
			assertArrayEquals(legacyExpected, legacyResult);

			ColumnReference[] references = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, ColumnReference.class))
					.toArray(ColumnReference[]::new);
			ColumnReference[] referencesExpected =
					new ColumnReference[]{null, null,
							new ColumnReference(set.getAttributes().getPredictedLabel().getName(), "Yes"), null,
							null, null, null, null,
							null, null, null, null, new ColumnReference(set.getAttributes().getPredictedLabel().getName()),
							null, null};
			assertArrayEquals(referencesExpected, references);
		}

		@Test
		public void testAnnotations() {
			Attribute attribute1 = attributeInt();
			Attribute attribute2 = attributeReal();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(10)
					.withColumnFiller(attribute1, i -> i + 1).withColumnFiller(attribute2, i -> i + 1.7).build();
			set.getAnnotations().setAnnotation(Annotations.KEY_DC_AUTHOR, "gmeier");

			IOTable table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT);

			assertEquals(set.getAnnotations(), table.getAnnotations());
		}
	}

	@RunWith(Parameterized.class)
	public static class TableToSet {

		public TableToSet(boolean legacyMode) {
			ParameterService.setParameterValue(RapidMiner.PROPERTY_RAPIDMINER_SYSTEM_LEGACY_DATA_MGMT,
					String.valueOf(legacyMode));
		}

		@Parameters(name = "legacyMode={0}")
		public static Collection<Object> params() {
			return Arrays.asList(true, false);
		}

		@Test
		public void testSimple() {
			Table table = Builders.newTableBuilder(112).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominal() {
			NominalBuffer buffer = BufferAccessor.get().newUInt8Buffer(ColumnType.NOMINAL, 112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}
			NominalBuffer buffer2 = BufferAccessor.get().newUInt8Buffer(ColumnType.NOMINAL, 112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn())
					.add("second", buffer2.toColumn())
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominalGaps() {
			NominalBuffer buffer = Buffers.nominalBuffer(11);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(7, null);
			buffer.set(5, null);
			NominalBuffer buffer2 = Buffers.nominalBuffer(11);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + i);
			}
			buffer2.set(3, null);
			buffer2.set(5, null);
			Column column = Columns.removeUnusedDictionaryValues(buffer.toColumn(),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Column column2 = Columns.removeUnusedDictionaryValues(buffer2.toColumn(),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Table table = Builders.newTableBuilder(11).add("first", column)
					.add("second", column2)
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testBinominal() {
			NominalBuffer buffer = Buffers.nominalBuffer(112, 2);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 2));
			}
			buffer.set(100, null);
			NominalBuffer buffer2 = Buffers.nominalBuffer(112, 2);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 2));
			}
			buffer2.set(42, null);
			NominalBuffer buffer3 = Buffers.nominalBuffer(112, 2);
			for (int i = 0; i < buffer.size(); i += 2) {
				buffer3.set(i, "one");
			}
			NominalBuffer buffer4 = Buffers.nominalBuffer(112, 2);

			Table table = Builders.newTableBuilder(112).add("first", buffer.toBooleanColumn( "value0"))
					.add("second", buffer2.toBooleanColumn( "val1"))
					.add("onePositive", buffer3.toBooleanColumn( "one"))
					.add("oneNegative", buffer3.toBooleanColumn( null))
					.add("empty", buffer4.toBooleanColumn( null))
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);

			NominalMapping first = set.getAttributes().get("first").getMapping();
			assertEquals("value1", first.getNegativeString());
			assertEquals("value0", first.getPositiveString());

			NominalMapping second = set.getAttributes().get("second").getMapping();
			assertEquals("val0", second.getNegativeString());
			assertEquals("val1", second.getPositiveString());

			NominalMapping oneNegative = set.getAttributes().get("oneNegative").getMapping();
			assertEquals("one", oneNegative.getNegativeString());
			assertNull(oneNegative.getPositiveString());

			NominalMapping empty = set.getAttributes().get("empty").getMapping();
			assertNull(empty.getPositiveString());
			assertNull(empty.getNegativeString());

			int[] valueTypes =
					Arrays.stream(set.getAttributes().createRegularAttributeArray()).mapToInt(Attribute::getValueType).toArray();
			assertArrayEquals(new int[]{Ontology.BINOMINAL, Ontology.BINOMINAL, Ontology.NOMINAL,
					Ontology.BINOMINAL, Ontology.BINOMINAL}, valueTypes);
		}

		@Test
		public void testBinominalGaps() {
			NominalBuffer buffer = BufferAccessor.get().newUInt2Buffer(ColumnType.NOMINAL, 112);
			buffer.set(0, "bla");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "blup");
			}
			buffer.set(100, null);
			NominalBuffer buffer2 = BufferAccessor.get().newUInt2Buffer(ColumnType.NOMINAL, 112);
			buffer2.set(0, "bla");
			for (int i = 0; i < buffer.size(); i++) {
				buffer2.set(i, "blup");
			}
			buffer2.set(100, null);

			Column bla = Columns.removeUnusedDictionaryValues(buffer.toBooleanColumn( "bla"),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Column blup = Columns.removeUnusedDictionaryValues(buffer2.toBooleanColumn("blup"),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Table table = Builders.newTableBuilder(112).add("first", bla)
					.add("second", blup)
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);

			int[] valueTypes =
					Arrays.stream(set.getAttributes().createRegularAttributeArray()).mapToInt(Attribute::getValueType).toArray();
			assertArrayEquals(new int[]{Ontology.BINOMINAL, Ontology.NOMINAL}, valueTypes);

			NominalMapping first = set.getAttributes().get("first").getMapping();
			assertEquals("blup", first.getNegativeString());
			assertNull(first.getPositiveString());

		}

		@Test
		public void testNominalUnusedValue() {
			NominalBuffer buffer = Buffers.nominalBuffer(112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "valu" + (i % 5));
			}
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}

			NominalBuffer buffer2 = Buffers.nominalBuffer(112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn())
					.add("second", buffer2.toColumn())
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testManyColumns() {
			TableBuilder builder = Builders.newTableBuilder(11);
			for (int i = 0; i < 30; i++) {
				builder.addReal("real" + i, j -> 3 * j / 5.0).addInt53Bit("int" + i, j -> 5 * j);
			}
			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testRoles() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addInt53Bit("att-1", i -> i);

			ColumnRole[] columnRoles = new ColumnRole[]{ColumnRole.ID, ColumnRole.LABEL, ColumnRole.PREDICTION,
					ColumnRole.SCORE, ColumnRole.WEIGHT, ColumnRole.OUTLIER, ColumnRole.CLUSTER, ColumnRole.BATCH,
					ColumnRole.METADATA};
			for (int i = 0; i < columnRoles.length; i++) {
				builder.addReal("att" + i, j -> j);
				builder.addMetaData("att" + i, columnRoles[i]);
			}

			builder.addInt53Bit("batt1", i -> i);
			builder.addMetaData("batt1", ColumnRole.METADATA);
			builder.addMetaData("batt1", new com.rapidminer.belt.table.LegacyRole("ignore-me"));

			builder.addInt53Bit("batt2", i -> i);
			builder.addMetaData("batt2", ColumnRole.SCORE);
			builder.addMetaData("batt2", new com.rapidminer.belt.table.LegacyRole("confidence_Yes"));

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			Iterable<AttributeRole> iterable = () -> set.getAttributes().allAttributeRoles();
			String[] result = StreamSupport.stream(iterable.spliterator(), false).map(AttributeRole::getSpecialName)
					.toArray(String[]::new);
			String[] expected =
					new String[]{null, Attributes.ID_NAME, Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
							Attributes.CONFIDENCE_NAME, Attributes.WEIGHT_NAME, Attributes.OUTLIER_NAME,
							Attributes.CLUSTER_NAME, Attributes.BATCH_NAME, "metadata", "ignore-me",
							"confidence_Yes"};
			assertArrayEquals(expected, result);
		}

		@Test
		public void testTypes() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addReal("att1", i -> i);

			builder.addReal("att2", i -> i);
			builder.addMetaData("att2", com.rapidminer.belt.table.LegacyType.NUMERICAL);

			builder.addInt53Bit("att3", i -> i);

			builder.addInt53Bit("att4", i -> i);
			builder.addMetaData("att4", com.rapidminer.belt.table.LegacyType.NUMERICAL);

			builder.addDateTime("att5", i -> Instant.EPOCH);

			builder.addDateTime("att6", i -> Instant.EPOCH);
			builder.addMetaData("att6", com.rapidminer.belt.table.LegacyType.DATE);

			builder.addDateTime("att6.5", i -> Instant.EPOCH);
			builder.addMetaData("att6.5", com.rapidminer.belt.table.LegacyType.TIME);

			builder.addTime("att7", i -> LocalTime.NOON);

			builder.addTime("att7.5", i -> LocalTime.NOON);
			builder.addMetaData("att7.5", com.rapidminer.belt.table.LegacyType.NUMERICAL);

			builder.addNominal("att8", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att8", com.rapidminer.belt.table.LegacyType.POLYNOMINAL);

			builder.addNominal("att9", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att9", com.rapidminer.belt.table.LegacyType.POLYNOMINAL);

			builder.addNominal("att10", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att10", com.rapidminer.belt.table.LegacyType.BINOMINAL);

			builder.addNominal("att11", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att11", com.rapidminer.belt.table.LegacyType.STRING);

			builder.addNominal("att12", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att12", com.rapidminer.belt.table.LegacyType.FILE_PATH);

			builder.addNominal("att13", i -> i % 2 == 0 ? "A" : "B", 2);

			builder.addBoolean("att14", i -> i % 2 == 0 ? "A" : "B", "A");

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			int[] result =
					StreamSupport.stream(set.getAttributes().spliterator(), false).mapToInt(Attribute::getValueType)
							.toArray();
			int[] expected = new int[]{Ontology.REAL, Ontology.NUMERICAL, Ontology.INTEGER, Ontology.INTEGER,
					Ontology.DATE_TIME, Ontology.DATE, Ontology.TIME, Ontology.TIME, Ontology.TIME, Ontology.POLYNOMINAL, Ontology.POLYNOMINAL,
					Ontology.BINOMINAL, Ontology.STRING, Ontology.FILE_PATH, Ontology.NOMINAL, Ontology.BINOMINAL};

			assertArrayEquals(expected, result);
		}

		@Test
		public void testInvalidLegacyTypes() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addReal("att1", i -> i);
			builder.addMetaData("att1", com.rapidminer.belt.table.LegacyType.DATE_TIME);

			builder.addReal("att2", i -> i);
			builder.addMetaData("att2", com.rapidminer.belt.table.LegacyType.INTEGER);

			builder.addInt53Bit("att3", i -> i);
			builder.addMetaData("att3", com.rapidminer.belt.table.LegacyType.REAL);

			builder.addInt53Bit("att4", i -> i);
			builder.addMetaData("att4", com.rapidminer.belt.table.LegacyType.POLYNOMINAL);

			builder.addNominal("att5", i -> i % 2 == 0 ? "A" : i % 3 == 0 ? "B" : "C", 2);
			builder.addMetaData("att5", com.rapidminer.belt.table.LegacyType.BINOMINAL);

			builder.addTime("att6", i -> LocalTime.NOON);
			builder.addMetaData("att6", com.rapidminer.belt.table.LegacyType.TIME);

			builder.addTime("att7", i -> LocalTime.NOON);
			builder.addMetaData("att7", com.rapidminer.belt.table.LegacyType.DATE);

			builder.addTime("att8", i -> LocalTime.NOON);
			builder.addMetaData("att8", com.rapidminer.belt.table.LegacyType.DATE_TIME);

			builder.addDateTime("att9", i -> Instant.EPOCH);
			builder.addMetaData("att9", com.rapidminer.belt.table.LegacyType.POLYNOMINAL);

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			int[] result =
					StreamSupport.stream(set.getAttributes().spliterator(), false).mapToInt(Attribute::getValueType)
							.toArray();
			int[] expected = new int[]{Ontology.REAL, Ontology.REAL, Ontology.INTEGER, Ontology.INTEGER,
					Ontology.NOMINAL, Ontology.TIME, Ontology.TIME, Ontology.TIME, Ontology.DATE_TIME};
			assertArrayEquals(expected, result);
		}

		@Test
		public void testAnnotations() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.build(Belt.defaultContext());

			IOTable tableObject = new IOTable(table);
			tableObject.getAnnotations().setAnnotation(Annotations.KEY_DC_AUTHOR, "gmeier");

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(tableObject, CONTEXT);

			assertEquals(tableObject.getAnnotations(), set.getAnnotations());
		}


		@Test(expected = BeltConverter.ConversionException.class)
		public void testAdvancedColumns() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.addTextset("textset", i -> new StringSet(Collections.singleton("val"+i)))
					.build(Belt.defaultContext());

			IOTable tableObject = new IOTable(table);
			try {
				BeltConverter.convert(tableObject, CONTEXT);
			} catch (BeltConverter.ConversionException e) {
				assertEquals("textset", e.getColumnName());
				assertEquals(ColumnType.TEXTSET, e.getType());
				throw e;
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class InputDetection {

		@Parameter
		public String inputType;

		@Parameter(1)
		public ExampleSet input;

		@Parameters(name = "{0}")
		public static Iterable<Object[]> inputClasses() throws ExpressionEvaluationException {
			Attribute attribute1 = attributeInt();
			Attribute attribute2 = attributeReal();

			// Simple example set with no logic in the view
			ExampleSet simpleSet = ExampleSets.from(attribute1, attribute2)
					.withBlankSize(200)
					.withColumnFiller(attribute1, i -> i + 1)
					.withColumnFiller(attribute2, i -> i + 1.7)
					.build();

			// Complex example sets that are considered thread-safe
			ExampleSet conditionedSet = new ConditionedExampleSet(simpleSet, new Condition() {
				@Override
				public boolean conditionOk(Example example) throws ExpressionEvaluationException {
					return example.getValue(attribute1) < 100 && example.getValue(attribute2) < 100;
				}

				@Override
				public Condition duplicate() {
					return null;
				}
			});

			ExampleSet sortedSet = new SortedExampleSet(simpleSet, attribute1, SortedExampleSet.DECREASING);

			ExampleSet mappedSet = new MappedExampleSet(simpleSet, new int[]{
					133, 156, 16, 0, 20, 199, 29, 192,
					185, 33, 175, 58, 15, 100, 2, 68,
					9, 122, 87, 84, 64, 56, 83, 177,
					39, 90, 112, 66, 90, 17, 95, 25}
			);

			// Complex example set that is not considered thread-safe
			Partition partition = new Partition(new double[]{0.25, 0.5, 0.25}, 200, new SimplePartitionBuilder());
			SplittedExampleSet splittedSet = new SplittedExampleSet(simpleSet, partition);

			// Simple example set with a attribute transformation that is considered unsafe
			ExampleSet transformationSet = (ExampleSet) simpleSet.clone();
			Attribute clonedAttribute = transformationSet.getAttributes().get(attribute1.getName());
			clonedAttribute.addTransformation(new AttributeTransformation() {
				@Override
				public double transform(Attribute attribute, double value) {
					return value * 42;
				}

				@Override
				public double inverseTransform(Attribute attribute, double value) {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean isReversable() {
					return false;
				}

				@Override
				public Object clone() {
					return this;
				}
			});

			return Arrays.asList(new Object[][]{
					{simpleSet.getClass().getSimpleName(), simpleSet},
					{conditionedSet.getClass().getSimpleName(), conditionedSet},
					{sortedSet.getClass().getSimpleName(), sortedSet},
					{mappedSet.getClass().getSimpleName(), mappedSet},
					{splittedSet.getClass().getSimpleName(), splittedSet},
					{AttributeTransformation.class.getSimpleName(), transformationSet}
			});
		}

		@Test
		public void testInputs() {
			Table table = com.rapidminer.belt.table.BeltConverter.convert(input, CONTEXT).getTable();
			double[][] result = readTableToArray(table);
			double[][] expected = readExampleSetToArray(input);
			assertArrayEquals(expected, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class BackAndForth {

		@BeforeClass
		public static void setup() {
			RapidAssert.ASSERTER_REGISTRY.registerAllAsserters(new AsserterFactoryRapidMiner());
		}

		public BackAndForth(boolean legacyMode) {
			ParameterService.setParameterValue(RapidMiner.PROPERTY_RAPIDMINER_SYSTEM_LEGACY_DATA_MGMT,
					String.valueOf(legacyMode));
		}

		@Parameters(name = "legacyMode={0}")
		public static Collection<Object> params() {
			return Arrays.asList(true, false);
		}

		@Test
		public void testAllTypes() {
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 1; i < Ontology.VALUE_TYPE_NAMES.length; i++) {
				attributes.add(AttributeFactory.createAttribute(i));
			}
			ExampleSet set = ExampleSets.from(attributes)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
			assertAttributeOrder(set, backSet);
		}

		@Test
		public void testAllTypesView() {
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 1; i < Ontology.VALUE_TYPE_NAMES.length; i++) {
				attributes.add(AttributeFactory.createAttribute(i));
			}
			ExampleSet set = new SortedExampleSet(ExampleSets.from(attributes)
					.build(), attributes.get(1), SortedExampleSet.DECREASING);

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
			assertAttributeOrder(set, backSet);
		}

		@Test
		public void testRoles() {
			Attribute integer = attributeInt();
			Attribute animals = attributeDogCatMouse();
			Attribute real = attributeReal();
			Attribute answer = attributeYesNo();
			Attribute confidence = attributeReal();
			confidence.setName("confidence");
			Attribute cluster = attributeReal();
			cluster.setName("cluster");
			List<Attribute> attributes = Arrays.asList(integer, animals, real, answer, confidence, cluster);

			ExampleSet set = ExampleSets.from(attributes).withBlankSize(10)
					.withRole(integer, Attributes.CONFIDENCE_NAME + "_" + "Yes")
					.withRole(answer, Attributes.LABEL_NAME)
					.withRole(confidence, "confidence(yes)")
					.withRole(cluster, "cluster_1_probability")
					.withRole(animals, "someStupidRole").build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
			assertAttributeOrder(set, backSet);
		}

		@Test
		public void testNumericTypes() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, integer, dateTime, date, time);

			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime, i -> Math.random() > 0.7 ? Double.NaN : (i % 3 == 0 ? -1 : 1)
							* 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN : (i % 3 == 0 ? -1 : 1) *
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time, i -> Math.random() > 0.7 ? Double.NaN : randomTimeMillis())
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
			assertAttributeOrder(set, backSet);
		}

		@Test
		public void testNominalTypes() {
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute string = AttributeFactory.createAttribute("string", Ontology.STRING);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			Attribute path = AttributeFactory.createAttribute("path", Ontology.FILE_PATH);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 4; i++) {
				string.getMapping().mapString("veryVeryLongStringValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			for (int i = 0; i < 3; i++) {
				path.getMapping().mapString("//folder/sufolder/subsubfolder/file" + i);
			}

			List<Attribute> attributes = Arrays.asList(nominal, string, polynominal, binominal, path);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(50)
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(string, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(4))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withColumnFiller(path, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(3))
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
			assertAttributeOrder(set, backSet);
		}

		@Test
		public void testIncompleteBinominalTypes() {
			Attribute binominalOne = AttributeFactory.createAttribute("binominalOne", Ontology.BINOMINAL);
			Attribute binominalZero = AttributeFactory.createAttribute("binominalZero", Ontology.BINOMINAL);
			binominalOne.getMapping().mapString("binominalValue" + 1);

			List<Attribute> attributes = Arrays.asList(binominalOne, binominalZero);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(50)
					.withColumnFiller(binominalOne, i -> random.nextDouble() > 0.7 ? Double.NaN : 0)
					.withColumnFiller(binominalZero, i -> Double.NaN)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
			assertAttributeOrder(set, backSet);
		}

		@Test
		public void testAttributeOrder() {
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 1; i < Ontology.VALUE_TYPE_NAMES.length; i++) {
				attributes.add(AttributeFactory.createAttribute(i));
			}
			ExampleSet set = ExampleSets.from(attributes)
					.build();
			//reoder attributes and include specials
			set.getAttributes().setSpecialAttribute(attributes.get(2), Attributes.LABEL_NAME);
			set.getAttributes().setSpecialAttribute(attributes.get(1), Attributes.CLUSTER_NAME);
			set.getAttributes().remove(attributes.get(0));
			set.getAttributes().addRegular(attributes.get(0));
			set.getAttributes().remove(attributes.get(4));
			set.getAttributes().addRegular(attributes.get(4));
			set.getAttributes().remove(attributes.get(6));
			set.getAttributes().addRegular(attributes.get(6));
			set.getAttributes().remove(attributes.get(5));
			set.getAttributes().addRegular(attributes.get(5));

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);

			assertAttributeOrder(set, backSet);
		}

	}

	/**
	 * Asserts that the order of the attributes is the same.
	 */
	static void assertAttributeOrder(ExampleSet expected, ExampleSet actual) {
		RapidAssert.assertEquals(getOrderedAttributeNames(expected), getOrderedAttributeNames(actual));
	}

	private static List<String> getOrderedAttributeNames(ExampleSet exampleSet) {
		List<String> names = new ArrayList<>();
		for (Iterator<Attribute> it = exampleSet.getAttributes().allAttributes(); it.hasNext(); ) {
			names.add(it.next().getName());
		}
		return names;
	}

	@RunWith(Parameterized.class)
	public static class TableToHeaderSet {

		@BeforeClass
		public static void setup() {
			RapidAssert.ASSERTER_REGISTRY.registerAllAsserters(new AsserterFactoryRapidMiner());
		}

		public TableToHeaderSet(boolean legacyMode) {
			ParameterService.setParameterValue(RapidMiner.PROPERTY_RAPIDMINER_SYSTEM_LEGACY_DATA_MGMT,
					String.valueOf(legacyMode));
		}

		@Parameters(name = "legacyMode={0}")
		public static Collection<Object> params() {
			return Arrays.asList(true, false);
		}


		@Test
		public void testNominalTypes() {
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute string = AttributeFactory.createAttribute("string", Ontology.STRING);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			Attribute path = AttributeFactory.createAttribute("path", Ontology.FILE_PATH);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 4; i++) {
				string.getMapping().mapString("veryVeryLongStringValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			for (int i = 0; i < 3; i++) {
				path.getMapping().mapString("//folder/sufolder/subsubfolder/file" + i);
			}

			List<Attribute> attributes = Arrays.asList(nominal, string, polynominal, binominal, path);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(50)
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(string, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(4))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withColumnFiller(path, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(3))
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			HeaderExampleSet headerSet = com.rapidminer.belt.table.BeltConverter.convertHeader(table);

			int[] oldValueTypes = Arrays.stream(set.getAttributes().createRegularAttributeArray())
					.mapToInt(Attribute::getValueType).toArray();
			int[] headerValueTypes = Arrays.stream(headerSet.getAttributes().createRegularAttributeArray())
					.mapToInt(Attribute::getValueType).toArray();
			assertArrayEquals(oldValueTypes, headerValueTypes);

			ExampleSet remapped = RemappedExampleSet.create(set, headerSet, false, true);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(remapped);
			assertArrayEquals(expected, result);

			double[][] expectedMapping = readTableToArray(table);
			double[][] resultMapping = readExampleSetToArray(remapped);
			assertArrayEquals(expectedMapping, resultMapping);
		}

		@Test
		public void testNumericTypes() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, integer, dateTime, date, time);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(50).withRole(integer, Attributes.LABEL_NAME).build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			HeaderExampleSet headerExampleSet = com.rapidminer.belt.table.BeltConverter.convertHeader(table);

			int[] oldValueTypes = Arrays.stream(set.getAttributes().createRegularAttributeArray())
					.mapToInt(Attribute::getValueType).toArray();
			int[] headerValueTypes = Arrays.stream(headerExampleSet.getAttributes().createRegularAttributeArray())
					.mapToInt(Attribute::getValueType).toArray();
			assertArrayEquals(oldValueTypes, headerValueTypes);
		}

		@Test
		public void testRemappingSame() {
			NominalBuffer buffer = BufferAccessor.get().newUInt16Buffer(ColumnType.NOMINAL, 112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}
			NominalBuffer buffer2 = BufferAccessor.get().newUInt16Buffer(ColumnType.NOMINAL, 112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn())
					.add("second", buffer2.toColumn())
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			HeaderExampleSet remappingSet = com.rapidminer.belt.table.BeltConverter.convertHeader(table);

			ExampleSet remapped = RemappedExampleSet.create(set, remappingSet, false, true);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(remapped);
			assertArrayEquals(expected, result);

			double[][] expectedMapping = readTableToArray(table);
			double[][] resultMapping = readExampleSetToArray(remapped);
			assertArrayEquals(expectedMapping, resultMapping);
		}

		@Test
		public void testRemappingUnusedValue() {
			NominalBuffer buffer = BufferAccessor.get().newUInt16Buffer(ColumnType.NOMINAL, 112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "valu" + (i % 5));
			}
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}

			NominalBuffer buffer2 = BufferAccessor.get().newInt32Buffer(ColumnType.NOMINAL, 112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn())
					.add("second", buffer2.toColumn())
					.build(Belt.defaultContext());

			buffer = BufferAccessor.get().newUInt16Buffer(ColumnType.NOMINAL, 112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}

			Table table2 = Builders.newTableBuilder(112).add("first", buffer.toColumn())
					.add("second", buffer2.toColumn())
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convert(new IOTable(table), CONTEXT);

			HeaderExampleSet remappingSet = com.rapidminer.belt.table.BeltConverter.convertHeader(table2);

			ExampleSet remapped = RemappedExampleSet.create(set, remappingSet, false, true);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(remapped);
			assertArrayEquals(expected, result);

			double[][] expectedMapping = readTableToArray(table2);
			double[][] resultMapping = readExampleSetToArray(remapped);
			assertArrayEquals(expectedMapping, resultMapping);
		}


		@Test(expected = BeltConverter.ConversionException.class)
		public void testAdvancedColumns() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.addTextset("textset", i -> new StringSet(Collections.singleton("val"+i)))
					.build(Belt.defaultContext());

			try {
				BeltConverter.convertHeader(table);
			} catch (BeltConverter.ConversionException e) {
				assertEquals("textset", e.getColumnName());
				assertEquals(ColumnType.TEXTSET, e.getType());
				throw e;
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class TableToSetSequentially {

		public TableToSetSequentially(boolean legacyMode) {
			ParameterService.setParameterValue(RapidMiner.PROPERTY_RAPIDMINER_SYSTEM_LEGACY_DATA_MGMT,
					String.valueOf(legacyMode));
		}

		@Parameters(name = "legacyMode={0}")
		public static Collection<Object> params() {
			return Arrays.asList(true, false);
		}

		@Test
		public void testSimple() {
			Table table = Builders.newTableBuilder(112).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convertSequentially(new IOTable(table));

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}


		@Test
		public void testManyColumns() {
			TableBuilder builder = Builders.newTableBuilder(11);
			for (int i = 0; i < 30; i++) {
				builder.addReal("real" + i, j -> 3 * j / 5.0).addInt53Bit("int" + i, j -> 5 * j);
			}
			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convertSequentially(new IOTable(table));

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testRoles() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addInt53Bit("att-1", i -> i);

			ColumnRole[] columnRoles = new ColumnRole[]{ColumnRole.ID, ColumnRole.LABEL, ColumnRole.PREDICTION,
					ColumnRole.SCORE, ColumnRole.WEIGHT, ColumnRole.OUTLIER, ColumnRole.CLUSTER, ColumnRole.BATCH,
					ColumnRole.METADATA};
			for (int i = 0; i < columnRoles.length; i++) {
				builder.addReal("att" + i, j -> j);
				builder.addMetaData("att" + i, columnRoles[i]);
			}

			builder.addInt53Bit("batt1", i -> i);
			builder.addMetaData("batt1", ColumnRole.METADATA);
			builder.addMetaData("batt1", new com.rapidminer.belt.table.LegacyRole("ignore-me"));

			builder.addInt53Bit("batt2", i -> i);
			builder.addMetaData("batt2", ColumnRole.SCORE);
			builder.addMetaData("batt2", new com.rapidminer.belt.table.LegacyRole("confidence_Yes"));

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convertSequentially(new IOTable(table));

			Iterable<AttributeRole> iterable = () -> set.getAttributes().allAttributeRoles();
			String[] result = StreamSupport.stream(iterable.spliterator(), false).map(AttributeRole::getSpecialName)
					.toArray(String[]::new);
			String[] expected =
					new String[]{null, Attributes.ID_NAME, Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
							Attributes.CONFIDENCE_NAME, Attributes.WEIGHT_NAME, Attributes.OUTLIER_NAME,
							Attributes.CLUSTER_NAME, Attributes.BATCH_NAME, "metadata", "ignore-me",
							"confidence_Yes"};
			assertArrayEquals(expected, result);
		}

		@Test
		public void testTypes() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addReal("att1", i -> i);

			builder.addReal("att2", i -> i);
			builder.addMetaData("att2", com.rapidminer.belt.table.LegacyType.NUMERICAL);

			builder.addInt53Bit("att3", i -> i);

			builder.addInt53Bit("att4", i -> i);
			builder.addMetaData("att4", com.rapidminer.belt.table.LegacyType.NUMERICAL);

			builder.addDateTime("att5", i -> Instant.EPOCH);

			builder.addDateTime("att6", i -> Instant.EPOCH);
			builder.addMetaData("att6", com.rapidminer.belt.table.LegacyType.DATE);

			builder.addDateTime("att6.5", i -> Instant.EPOCH);
			builder.addMetaData("att6.5", com.rapidminer.belt.table.LegacyType.TIME);

			builder.addTime("att7", i -> LocalTime.NOON);

			builder.addTime("att7.5", i -> LocalTime.NOON);
			builder.addMetaData("att7.5", com.rapidminer.belt.table.LegacyType.NUMERICAL);

			builder.addNominal("att8", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att8", com.rapidminer.belt.table.LegacyType.POLYNOMINAL);

			builder.addNominal("att9", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att9", com.rapidminer.belt.table.LegacyType.POLYNOMINAL);

			builder.addNominal("att10", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att10", com.rapidminer.belt.table.LegacyType.BINOMINAL);

			builder.addNominal("att11", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att11", com.rapidminer.belt.table.LegacyType.STRING);

			builder.addNominal("att12", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att12", com.rapidminer.belt.table.LegacyType.FILE_PATH);

			builder.addNominal("att13", i -> i % 2 == 0 ? "A" : "B", 2);

			builder.addBoolean("att14", i -> i % 2 == 0 ? "A" : "B", "A");

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.BeltConverter.convertSequentially(new IOTable(table));

			int[] result =
					StreamSupport.stream(set.getAttributes().spliterator(), false).mapToInt(Attribute::getValueType)
							.toArray();
			int[] expected = new int[]{Ontology.REAL, Ontology.NUMERICAL, Ontology.INTEGER, Ontology.INTEGER,
					Ontology.DATE_TIME, Ontology.DATE, Ontology.TIME, Ontology.TIME, Ontology.TIME, Ontology.POLYNOMINAL, Ontology.POLYNOMINAL,
					Ontology.BINOMINAL, Ontology.STRING, Ontology.FILE_PATH, Ontology.NOMINAL, Ontology.BINOMINAL};

			assertArrayEquals(expected, result);
		}

		@Test
		public void testAnnotations() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.build(Belt.defaultContext());

			IOTable tableObject = new IOTable(table);
			tableObject.getAnnotations().setAnnotation(Annotations.KEY_DC_AUTHOR, "gmeier");

			ExampleSet set = BeltConverter.convertSequentially(tableObject);

			assertEquals(tableObject.getAnnotations(), set.getAnnotations());
		}

		@Test(expected = BeltConverter.ConversionException.class)
		public void testAdvancedColumns() {
			Table table = Builders.newTableBuilder(11).addReal("real", i ->  3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.addTextset("textset", i -> new StringSet(Collections.singleton("val"+i)))
					.build(Belt.defaultContext());

			try {
				BeltConverter.convertSequentially(new IOTable(table));
			} catch (BeltConverter.ConversionException e) {
				assertEquals("textset", e.getColumnName());
				assertEquals(ColumnType.TEXTSET, e.getType());
				throw e;
			}
		}
	}

	/**
	 * Tests for the conversion between the old and new time format.
	 */
	public static class TimeConversion {
		@Test
		public void testFromLocalTime() {
			TableBuilder builder = Builders.newTableBuilder(2);
			LocalTime[] testValues = new LocalTime[]{
					LocalTime.MIN,
					LocalTime.MAX,
					LocalTime.MIDNIGHT,
					LocalTime.NOON,
					LocalTime.now()
			};
			builder.addTime("time", i -> testValues[i]);
			Table table = builder.build(Belt.defaultContext());
			ExampleSet es = BeltConverter.convertSequentially(new IOTable(table));
			Table convertedTable = BeltConverter.convert(es, CONTEXT).getTable();
			double[] expected = readTableToArray(convertedTable)[0];
			double[] actual = readTableToArray(convertedTable)[0];
			for (int i = 0; i < expected.length; i++) {
				assertEquals(((long) expected[i]) / 1_000_000 * 1_000_000, (long) actual[i]);
			}
		}

		@Test
		public void testFromLegacyTime() {
			ExampleSetBuilder builder = ExampleSets.from(AttributeFactory.createAttribute("time", Ontology.TIME));
			long millisecondsPerHour = 3_600_000L;
			for (int i = -24; i < 24; i++) {
				builder.addRow(new double[]{i * millisecondsPerHour});
				builder.addRow(new double[]{i * millisecondsPerHour + 1});
				builder.addRow(new double[]{i * millisecondsPerHour - 1});
			}
			ExampleSet es = builder.build();
			Table table = BeltConverter.convert(es, CONTEXT).getTable();
			double[] expected = readExampleSetToArray(es)[0];
			double[] actual = readTableToArray(table)[0];
			for (int i = 0; i < expected.length; i++) {
				LocalTime localTime = LocalTime.ofNanoOfDay((long) actual[i]);
				Calendar calendar = Tools.getPreferredCalendar();
				calendar.setTimeInMillis((long) expected[i]);
				assertEquals(calendar.get(Calendar.HOUR_OF_DAY), localTime.getHour());
				assertEquals(calendar.get(Calendar.MINUTE), localTime.getMinute());
				assertEquals(calendar.get(Calendar.SECOND), localTime.getSecond());
				assertEquals(calendar.get(Calendar.MILLISECOND), localTime.getNano() / 1_000_000);
				assertEquals(0, localTime.getNano() % 1_000_000);
			}
		}
	}

	public static class EpochMillis {

		@Test
		public void testEpoch() {
			assertEquals(Instant.EPOCH.toEpochMilli(), BeltConverter.toEpochMilli(Instant.EPOCH), 1e-20);
		}

		@Test
		public void testNormal() {
			Instant instant = Instant.ofEpochSecond(1607000031, 558000000);
			assertEquals(instant.toEpochMilli(), BeltConverter.toEpochMilli(instant), 1e-20);
		}

		@Test
		public void testNegativeRecent() {
			Instant instant = Instant.ofEpochSecond(-4704433617L, 558000000);
			assertEquals(instant.toEpochMilli(), BeltConverter.toEpochMilli(instant), 1e-20);
		}

		@Test
		public void testMin() {
			//No arithmetic exception
			BeltConverter.toEpochMilli(Instant.MIN);
		}

		@Test
		public void testMax() {
			//No arithmetic exception
			BeltConverter.toEpochMilli(Instant.MAX);
		}
	}

	@RunWith(Parameterized.class)
	public static class ToStudioRole {

		private final ColumnRole role;

		public ToStudioRole(ColumnRole role) {
			this.role = role;
		}

		@Parameterized.Parameters(name = "{0}")
		public static Collection<Object> params() {
			return Arrays.asList(ColumnRole.values());
		}

		@Test
		public void testConversion() {
			assertEquals(role, BeltConverter.convertRole(BeltConverter.toStudioRole(role)));
		}
	}

	public static class HeaderExampleTest {

		@BeforeClass
		public static void setup() {
			RapidMiner.initAsserters();
		}

		@Test
		public void testConversionSimple() {
			Table table = Builders.newTableBuilder(12)
					.addReal("real", i -> i / 2.0).addInt53Bit("int", i -> i)
					.addBoolean("bool", i -> i % 2 == 0 ? "bla" : "blup", "blup")
					.addNominal("nominal", i -> "val" + i)
					.addDateTime("datetime", i -> Instant.ofEpochSecond(i * 100000)).build(Belt.defaultContext());
			HeaderExampleSet examples = BeltConverter.convertHeader(table);
			IOTable convert = BeltConverter.convert(examples, new SequentialConcurrencyContext());
			RapidAssert.assertEquals(new IOTable(table.stripData()), convert);
		}

		@Test
		public void testConversionSpecialBoolean() {
			Table table = Builders.newTableBuilder(12)
					.addBoolean("bool0", i -> null, null)
					.addBoolean("bool", i -> i % 2 == 0 ? "bla" : "blup", "bla")
					.addBoolean("bool2", i -> i % 2 == 0 ? "bla" : null, "bla")
					.addBoolean("bool3", i -> i % 2 == 0 ? "bla" : null, null)
					.build(Belt.defaultContext());
			HeaderExampleSet examples = BeltConverter.convertHeader(table);
			IOTable convert = BeltConverter.convert(examples, new SequentialConcurrencyContext());
			RapidAssert.assertEquals(new IOTable(table.stripData()), convert);
		}

		@Test
		public void testFromES() {
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute string = AttributeFactory.createAttribute("string", Ontology.STRING);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			Attribute path = AttributeFactory.createAttribute("path", Ontology.FILE_PATH);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 4; i++) {
				string.getMapping().mapString("veryVeryLongStringValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			for (int i = 0; i < 3; i++) {
				path.getMapping().mapString("//folder/sufolder/subsubfolder/file" + i);
			}

			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes =
					Arrays.asList(nominal, string, polynominal, binominal, path, numeric, real, integer, dateTime,
							date, time);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(50)
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(string, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(4))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withColumnFiller(path, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(3))
					.withColumnFiller(real, i -> random.nextDouble())
					.withColumnFiller(integer, i -> random.nextInt())
					.build();
			IOTable table = BeltConverter.convert(set, new SequentialConcurrencyContext());
			HeaderExampleSet headerExampleSet = new HeaderExampleSet(set);
			IOTable expected = new IOTable(table.getTable().stripData());
			RapidAssert.assertEquals(expected, BeltConverter.convert(headerExampleSet, new SequentialConcurrencyContext()));
		}
	}

	public static Attribute attributeDogCatMouse() {
		Attribute a = AttributeFactory.createAttribute("animal", Ontology.NOMINAL);
		a.getMapping().mapString("dog");
		a.getMapping().mapString("cat");
		a.getMapping().mapString("mouse");
		return a;
	}

	public static Attribute attributeYesNo() {
		Attribute a = AttributeFactory.createAttribute("decision", Ontology.NOMINAL);
		a.getMapping().mapString("no");
		a.getMapping().mapString("yes");
		return a;
	}

	public static Attribute attributeInt() {
		return AttributeFactory.createAttribute("integer", Ontology.INTEGER);
	}

	public static Attribute attributeReal() {
		return AttributeFactory.createAttribute("real", Ontology.REAL);
	}

	public static Attribute attributeReal(int index) {
		return AttributeFactory.createAttribute("real" + index, Ontology.REAL);
	}

	/**
	 * Creates a random time on the day of epoch in the time zone represented via {@link Tools#getPreferredTimeZone()}.
	 * @return the random time in milliseconds of the day
	 */
	static long randomTimeMillis(){
		Calendar cal = Tools.getPreferredCalendar();
		cal.setTimeInMillis((long) Math.floor(Math.random() * 60 * 60 * 24 * 1000));
		cal.set(1970, Calendar.JANUARY, 1);
		return cal.getTimeInMillis();
	}
}