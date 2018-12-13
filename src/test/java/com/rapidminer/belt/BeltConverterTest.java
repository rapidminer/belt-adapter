/**
 * Copyright (C) 2001-2018 by RapidMiner and the contributors
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
package com.rapidminer.belt;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
import com.rapidminer.example.utils.ExampleSetBuilder;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.Annotations;
import com.rapidminer.operator.tools.ExpressionEvaluationException;
import com.rapidminer.test.asserter.AsserterFactoryRapidMiner;
import com.rapidminer.test_utils.RapidAssert;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.ParameterService;


/**
 * Tests the {@link BeltConverter}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class BeltConverterTest {

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
		List<String> categoricalMapping = col.getDictionary(String.class);
		CategoricalReader reader = Readers.categoricalReader(col);
		for (int j = 0; j < table.height(); j++) {
			data[j] = categoricalMapping.get(reader.read());
		}
		return data;
	}

	private static double[][] readTableToArray(Table table) {
		double[][] result = new double[table.width()][];
		Arrays.setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	private static String[][] readTableToStringArray(Table table) {
		String[][] result = new String[table.width()][];
		Arrays.setAll(result, i -> readColumnToStringArray(table, i));
		return result;
	}

	private static double[][] readExampleSetToArray(ExampleSet set) {
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
			BeltConverter.convert((ExampleSet) null, CONTEXT);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetToTableNullContext() {
			BeltConverter.convert(ExampleSetFactory.createExampleSet(new double[][]{new double[]{0}}), null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTableToSetNullTable() {
			BeltConverter.convert((IOTable) null, CONTEXT);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTableToSetSequentiallyNullTable() {
			BeltConverter.convertSequentially((IOTable) null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTableToSetNullContext() {
			BeltConverter.convert(new IOTable(Builders.newTableBuilder(1).build(Belt.defaultContext())), null);
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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			String[][] result = readTableToStringArray(table);
			String[][] expected = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominalUnusedValue() {
			Attribute attribute1 = attributeDogCatMouse();
			Attribute attribute2 = attributeYesNo();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(200)
					.withColumnFiller(attribute1, i -> i % 2).withColumnFiller(attribute2, i -> 1).build();
			set.getExample(10).setValue(attribute1, Double.NaN);
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			Column.TypeId[] result =
					table.labels().stream().map(label -> table.column(label).type().id()).toArray(Column
							.TypeId[]::new);
			Column.TypeId[] expected =
					new Column.TypeId[]{Column.TypeId.NOMINAL, Column.TypeId.REAL, Column.TypeId.INTEGER,
							Column.TypeId.REAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL,
							Column.TypeId.NOMINAL, Column.TypeId.DATE_TIME, Column.TypeId.DATE_TIME,
							Column.TypeId.DATE_TIME};
			assertArrayEquals(expected, result);

			LegacyType[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, LegacyType.class))
					.toArray(LegacyType[]::new);
			LegacyType[] legacyExpected =
					new LegacyType[]{LegacyType.NOMINAL, LegacyType.NUMERICAL, null, null,
							LegacyType.STRING, LegacyType.BINOMINAL, null,
							LegacyType.FILE_PATH, null, LegacyType.DATE, LegacyType.TIME};
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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			Column.TypeId[] result =
					table.labels().stream().map(label -> table.column(label).type().id()).toArray(Column
							.TypeId[]::new);
			Column.TypeId[] expected =
					new Column.TypeId[]{Column.TypeId.NOMINAL, Column.TypeId.REAL, Column.TypeId.INTEGER,
							Column.TypeId.REAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL, Column.TypeId.NOMINAL,
							Column.TypeId.NOMINAL, Column.TypeId.DATE_TIME, Column.TypeId.DATE_TIME,
							Column.TypeId.DATE_TIME};
			assertArrayEquals(expected, result);

			LegacyType[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, LegacyType.class))
					.toArray(LegacyType[]::new);
			LegacyType[] legacyExpected =
					new LegacyType[]{LegacyType.NOMINAL, LegacyType.NUMERICAL, null, null,
							LegacyType.STRING, LegacyType.BINOMINAL, null,
							LegacyType.FILE_PATH, null, LegacyType.DATE, LegacyType.TIME};
			assertArrayEquals(legacyExpected, legacyResult);
		}


		@Test
		public void testRoles() {
			String[] roles = new String[]{Attributes.ID_NAME, Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
					Attributes.CLUSTER_NAME, Attributes.WEIGHT_NAME, Attributes.BATCH_NAME, Attributes.OUTLIER_NAME,
					Attributes.CONFIDENCE_NAME,
					Attributes.CONFIDENCE_NAME + "_" + "Yes", Attributes.CLASSIFICATION_COST, "ignore-me"};
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 0; i < roles.length + 1; i++) {
				attributes.add(AttributeFactory.createAttribute(Ontology.NUMERICAL));
			}
			ExampleSetBuilder builder = ExampleSets.from(attributes);
			for (int i = 1; i < roles.length + 1; i++) {
				builder.withRole(attributes.get(i), roles[i - 1]);
			}
			ExampleSet set = builder.build();
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			ColumnRole[] result = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new);
			ColumnRole[] expected =
					new ColumnRole[]{null, ColumnRole.ID, ColumnRole.LABEL, ColumnRole.PREDICTION, ColumnRole.CLUSTER,
							ColumnRole.WEIGHT, ColumnRole.BATCH, ColumnRole.OUTLIER, ColumnRole.SCORE, ColumnRole
							.SCORE,
							ColumnRole.METADATA, ColumnRole.METADATA};
			assertArrayEquals(expected, result);

			LegacyRole[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, LegacyRole.class))
					.toArray(LegacyRole[]::new);
			LegacyRole[] legacyExpected =
					new LegacyRole[]{null, null, null, null, null, null, null, null, new LegacyRole("confidence"),
							new LegacyRole("confidence_Yes"),
							new LegacyRole(Attributes.CLASSIFICATION_COST), new LegacyRole("ignore-me")};
			assertArrayEquals(legacyExpected, legacyResult);
		}

		@Test
		public void testRolesView() {
			String[] roles = new String[]{Attributes.ID_NAME, Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
					Attributes.CLUSTER_NAME, Attributes.WEIGHT_NAME, Attributes.BATCH_NAME, Attributes.OUTLIER_NAME,
					Attributes.CONFIDENCE_NAME, Attributes.CONFIDENCE_NAME + "_" + "Yes",
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
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			ColumnRole[] result = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, ColumnRole.class))
					.toArray(ColumnRole[]::new);
			ColumnRole[] expected =
					new ColumnRole[]{null, ColumnRole.ID, ColumnRole.LABEL, ColumnRole.PREDICTION, ColumnRole.CLUSTER,
							ColumnRole.WEIGHT, ColumnRole.BATCH, ColumnRole.OUTLIER, ColumnRole.SCORE, ColumnRole
							.SCORE,
							ColumnRole.METADATA, ColumnRole.METADATA};
			assertArrayEquals(expected, result);

			LegacyRole[] legacyResult = table.labels().stream()
					.map(label -> table.getFirstMetaData(label, LegacyRole.class))
					.toArray(LegacyRole[]::new);
			LegacyRole[] legacyExpected =
					new LegacyRole[]{null, null, null, null, null, null, null, null, new LegacyRole("confidence"),
							new LegacyRole("confidence_Yes"),
							new LegacyRole(Attributes.CLASSIFICATION_COST), new LegacyRole("ignore-me")};
			assertArrayEquals(legacyExpected, legacyResult);
		}

		@Test
		public void testAnnotations() {
			Attribute attribute1 = attributeInt();
			Attribute attribute2 = attributeReal();
			ExampleSet set = ExampleSets.from(attribute1, attribute2).withBlankSize(10)
					.withColumnFiller(attribute1, i -> i + 1).withColumnFiller(attribute2, i -> i + 1.7).build();
			set.getAnnotations().setAnnotation(Annotations.KEY_DC_AUTHOR, "gmeier");

			IOTable table = BeltConverter.convert(set, CONTEXT);

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
			Table table = Builders.newTableBuilder(112).addReal("real", i -> 3 * i / 5.0).addInt("int", i -> 5 * i)
					.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominal() {
			UInt8CategoricalBuffer<String> buffer = new UInt8CategoricalBuffer<>(112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}
			UInt8CategoricalBuffer<String> buffer2 = new UInt8CategoricalBuffer<>(112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn(ColumnTypes.NOMINAL))
					.add("second", buffer2.toColumn(ColumnTypes.NOMINAL))
					.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testBinominal() {
			UInt2CategoricalBuffer<String> buffer = new UInt2CategoricalBuffer<>(112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 2));
			}
			buffer.set(100, null);
			UInt2CategoricalBuffer<String> buffer2 = new UInt2CategoricalBuffer<>(112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 2));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toBooleanColumn(ColumnTypes.NOMINAL, "value0"))
					.add("second", buffer2.toBooleanColumn(ColumnTypes.NOMINAL, "val1"))
					.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testNominalUnusedValue() {
			Int32CategoricalBuffer<String> buffer = new Int32CategoricalBuffer<>(112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "valu" + (i % 5));
			}
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}

			Int32CategoricalBuffer<String> buffer2 = new Int32CategoricalBuffer<>(112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn(ColumnTypes.NOMINAL))
					.add("second", buffer2.toColumn(ColumnTypes.NOMINAL))
					.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testManyColumns() {
			TableBuilder builder = Builders.newTableBuilder(11);
			for (int i = 0; i < 30; i++) {
				builder.addReal("real" + i, j -> 3 * j / 5.0).addInt("int" + i, j -> 5 * j);
			}
			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testRoles() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addInt("att-1", i -> i);

			ColumnRole[] columnRoles = new ColumnRole[]{ColumnRole.ID, ColumnRole.LABEL, ColumnRole.PREDICTION,
					ColumnRole.SCORE, ColumnRole.WEIGHT, ColumnRole.OUTLIER, ColumnRole.CLUSTER, ColumnRole.BATCH,
					ColumnRole.METADATA};
			for (int i = 0; i < columnRoles.length; i++) {
				builder.addReal("att" + i, j -> j);
				builder.addMetaData("att" + i, columnRoles[i]);
			}

			builder.addInt("batt1", i -> i);
			builder.addMetaData("batt1", ColumnRole.METADATA);
			builder.addMetaData("batt1", new LegacyRole("ignore-me"));

			builder.addInt("batt2", i -> i);
			builder.addMetaData("batt2", ColumnRole.SCORE);
			builder.addMetaData("batt2", new LegacyRole("confidence_Yes"));

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			Iterable<AttributeRole> iterable = () -> set.getAttributes().allAttributeRoles();
			String[] result = StreamSupport.stream(iterable.spliterator(), false).map(AttributeRole::getSpecialName)
					.toArray(String[]::new);
			String[] expected =
					new String[]{null, Attributes.ID_NAME, Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
							Attributes.CONFIDENCE_NAME, Attributes.WEIGHT_NAME, Attributes.OUTLIER_NAME,
							Attributes.CLUSTER_NAME, Attributes.BATCH_NAME, "meta_data", "ignore-me",
							"confidence_Yes"};
			assertArrayEquals(expected, result);
		}

		@Test
		public void testTypes() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addReal("att1", i -> i);

			builder.addReal("att2", i -> i);
			builder.addMetaData("att2", LegacyType.NUMERICAL);

			builder.addInt("att3", i -> i);

			builder.addInt("att4", i -> i);
			builder.addMetaData("att4", LegacyType.NUMERICAL);

			builder.addDateTime("att5", i -> Instant.EPOCH);

			builder.addDateTime("att6", i -> Instant.EPOCH);
			builder.addMetaData("att6", LegacyType.DATE);

			builder.addDateTime("att6.5", i -> Instant.EPOCH);
			builder.addMetaData("att6.5", LegacyType.TIME);

			builder.addTime("att7", i -> LocalTime.NOON);

			builder.addTime("att7.5", i -> LocalTime.NOON);
			builder.addMetaData("att7.5", LegacyType.NUMERICAL);

			builder.addNominal("att8", i -> i % 2 == 0 ? "A" : "B");

			builder.addNominal("att9", i -> i % 2 == 0 ? "A" : "B", 2);

			builder.addNominal("att10", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att10", LegacyType.BINOMINAL);

			builder.addNominal("att11", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att11", LegacyType.STRING);

			builder.addNominal("att12", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att12", LegacyType.FILE_PATH);

			builder.addNominal("att13", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att13", LegacyType.NOMINAL);

			builder.addBoolean("att14", i -> i % 2 == 0 ? "A" : "B", "A", ColumnTypes.NOMINAL);

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			int[] result =
					StreamSupport.stream(set.getAttributes().spliterator(), false).mapToInt(Attribute::getValueType)
							.toArray();
			int[] expected = new int[]{Ontology.REAL, Ontology.NUMERICAL, Ontology.INTEGER, Ontology.NUMERICAL,
					Ontology.DATE_TIME, Ontology.DATE, Ontology.TIME, Ontology.INTEGER, Ontology.NUMERICAL, Ontology.POLYNOMINAL, Ontology.POLYNOMINAL,
					Ontology.BINOMINAL,	Ontology.STRING, Ontology.FILE_PATH, Ontology.NOMINAL, Ontology.BINOMINAL};

			assertArrayEquals(expected, result);
		}

		@Test
		public void testInvalidLegacyTypes() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addReal("att1", i -> i);
			builder.addMetaData("att1", LegacyType.DATE_TIME);

			builder.addReal("att2", i -> i);
			builder.addMetaData("att2", LegacyType.INTEGER);

			builder.addInt("att3", i -> i);
			builder.addMetaData("att3", LegacyType.REAL);

			builder.addInt("att4", i -> i);
			builder.addMetaData("att4", LegacyType.NOMINAL);

			builder.addNominal("att5", i -> i % 2 == 0 ? "A" : i % 3 == 0 ? "B" : "C", 2);
			builder.addMetaData("att5", LegacyType.BINOMINAL);

			builder.addTime("att6", i -> LocalTime.NOON);
			builder.addMetaData("att6", LegacyType.TIME);

			builder.addTime("att7", i -> LocalTime.NOON);
			builder.addMetaData("att7", LegacyType.DATE);

			builder.addTime("att8", i -> LocalTime.NOON);
			builder.addMetaData("att8", LegacyType.DATE_TIME);

			builder.addDateTime("att9", i -> Instant.EPOCH);
			builder.addMetaData("att9", LegacyType.NOMINAL);

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			int[] result =
					StreamSupport.stream(set.getAttributes().spliterator(), false).mapToInt(Attribute::getValueType)
							.toArray();
			int[] expected = new int[]{Ontology.REAL, Ontology.REAL, Ontology.INTEGER, Ontology.INTEGER,
					Ontology.POLYNOMINAL, Ontology.INTEGER, Ontology.INTEGER, Ontology.INTEGER, Ontology.DATE_TIME};
			assertArrayEquals(expected, result);
		}

		@Test
		public void testAnnotations() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt("int", i -> 5 * i)
					.build(Belt.defaultContext());

			IOTable tableObject = new IOTable(table);
			tableObject.getAnnotations().setAnnotation(Annotations.KEY_DC_AUTHOR, "gmeier");

			ExampleSet set = BeltConverter.convert(tableObject, CONTEXT);

			assertEquals(tableObject.getAnnotations(), set.getAnnotations());
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

			return Arrays.asList(new Object[][] {
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
			Table table = BeltConverter.convert(input, CONTEXT).getTable();
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

			Table table = BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
		}

		@Test
		public void testAllTypesView() {
			List<Attribute> attributes = new ArrayList<>();
			for (int i = 1; i < Ontology.VALUE_TYPE_NAMES.length; i++) {
				attributes.add(AttributeFactory.createAttribute(i));
			}
			ExampleSet set = new SortedExampleSet(ExampleSets.from(attributes)
					.build(), attributes.get(1), SortedExampleSet.DECREASING);;

			Table table = BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
		}

		@Test
		public void testRoles() {
			Attribute integer = attributeInt();
			Attribute animals = attributeDogCatMouse();
			Attribute real = attributeReal();
			Attribute answer = attributeYesNo();
			List<Attribute> attributes = Arrays.asList(integer, animals, real, answer);

			ExampleSet set = ExampleSets.from(attributes).withBlankSize(10)
					.withRole(integer, Attributes.CONFIDENCE_NAME+"_"+"Yes")
					.withRole(answer, Attributes.LABEL_NAME)
					.withRole(animals, "someStupidRole").build();

			Table table = BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
		}

		@Test
		public void testNumericTypes() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes =Arrays.asList(numeric, real, integer, dateTime, date, time);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,	i -> Math.random() > 0.7 ? Double.NaN : (i % 3 == 0 ? -1 : 1)
							* 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :  (i % 3 == 0 ? -1 : 1) *
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time, i -> Math.random() > 0.7 ? Double.NaN :
							(i % 3 == 0 ? -1 : 1) * Math.floor(Math.random() * 60 * 60 * 24 * 1000))
					.build();

			Table table = BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
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

			Table table = BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
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

			Table table = BeltConverter.convert(set, CONTEXT).getTable();
			ExampleSet backSet = BeltConverter.convert(new IOTable(table), CONTEXT);
			RapidAssert.assertEquals(set, backSet);
		}


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

			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			HeaderExampleSet headerSet = BeltConverter.convertHeader(table);

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

			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			HeaderExampleSet headerExampleSet = BeltConverter.convertHeader(table);

			int[] oldValueTypes = Arrays.stream(set.getAttributes().createRegularAttributeArray())
					.mapToInt(Attribute::getValueType).toArray();
			int[] headerValueTypes = Arrays.stream(headerExampleSet.getAttributes().createRegularAttributeArray())
					.mapToInt(Attribute::getValueType).toArray();
			assertArrayEquals(oldValueTypes, headerValueTypes);
		}

		@Test
		public void testRemappingSame() {
			UInt16CategoricalBuffer<String> buffer = new UInt16CategoricalBuffer<>(112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}
			UInt16CategoricalBuffer<String> buffer2 = new UInt16CategoricalBuffer<>(112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn(ColumnTypes.NOMINAL))
					.add("second", buffer2.toColumn(ColumnTypes.NOMINAL))
					.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			HeaderExampleSet remappingSet = BeltConverter.convertHeader(table);

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
			Int32CategoricalBuffer<String> buffer = new Int32CategoricalBuffer<>(112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "valu" + (i % 5));
			}
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}

			Int32CategoricalBuffer<String> buffer2 = new Int32CategoricalBuffer<>(112);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + (i % 7));
			}
			buffer2.set(42, null);
			Table table = Builders.newTableBuilder(112).add("first", buffer.toColumn(ColumnTypes.NOMINAL))
					.add("second", buffer2.toColumn(ColumnTypes.NOMINAL))
					.build(Belt.defaultContext());

			buffer = new Int32CategoricalBuffer<>(112);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 5));
			}

			Table table2 = Builders.newTableBuilder(112).add("first", buffer.toColumn(ColumnTypes.NOMINAL))
					.add("second", buffer2.toColumn(ColumnTypes.NOMINAL))
					.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convert(new IOTable(table), CONTEXT);

			HeaderExampleSet remappingSet = BeltConverter.convertHeader(table2);

			ExampleSet remapped = RemappedExampleSet.create(set, remappingSet, false, true);

			String[][] expected = readTableToStringArray(table);
			String[][] result = readExampleSetToStringArray(remapped);
			assertArrayEquals(expected, result);

			double[][] expectedMapping = readTableToArray(table2);
			double[][] resultMapping = readExampleSetToArray(remapped);
			assertArrayEquals(expectedMapping, resultMapping);
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
			Table table = Builders.newTableBuilder(112).addReal("real", i -> 3 * i / 5.0).addInt("int", i -> 5 * i)
					.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convertSequentially(new IOTable(table));

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}


		@Test
		public void testManyColumns() {
			TableBuilder builder = Builders.newTableBuilder(11);
			for (int i = 0; i < 30; i++) {
				builder.addReal("real" + i, j -> 3 * j / 5.0).addInt("int" + i, j -> 5 * j);
			}
			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convertSequentially(new IOTable(table));

			double[][] expected = readTableToArray(table);
			double[][] result = readExampleSetToArray(set);
			assertArrayEquals(expected, result);
		}

		@Test
		public void testRoles() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addInt("att-1", i -> i);

			ColumnRole[] columnRoles = new ColumnRole[]{ColumnRole.ID, ColumnRole.LABEL, ColumnRole.PREDICTION,
					ColumnRole.SCORE, ColumnRole.WEIGHT, ColumnRole.OUTLIER, ColumnRole.CLUSTER, ColumnRole.BATCH,
					ColumnRole.METADATA};
			for (int i = 0; i < columnRoles.length; i++) {
				builder.addReal("att" + i, j -> j);
				builder.addMetaData("att" + i, columnRoles[i]);
			}

			builder.addInt("batt1", i -> i);
			builder.addMetaData("batt1", ColumnRole.METADATA);
			builder.addMetaData("batt1", new LegacyRole("ignore-me"));

			builder.addInt("batt2", i -> i);
			builder.addMetaData("batt2", ColumnRole.SCORE);
			builder.addMetaData("batt2", new LegacyRole("confidence_Yes"));

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convertSequentially(new IOTable(table));

			Iterable<AttributeRole> iterable = () -> set.getAttributes().allAttributeRoles();
			String[] result = StreamSupport.stream(iterable.spliterator(), false).map(AttributeRole::getSpecialName)
					.toArray(String[]::new);
			String[] expected =
					new String[]{null, Attributes.ID_NAME, Attributes.LABEL_NAME, Attributes.PREDICTION_NAME,
							Attributes.CONFIDENCE_NAME, Attributes.WEIGHT_NAME, Attributes.OUTLIER_NAME,
							Attributes.CLUSTER_NAME, Attributes.BATCH_NAME, "meta_data", "ignore-me",
							"confidence_Yes"};
			assertArrayEquals(expected, result);
		}

		@Test
		public void testTypes() {
			TableBuilder builder = Builders.newTableBuilder(10);
			builder.addReal("att1", i -> i);

			builder.addReal("att2", i -> i);
			builder.addMetaData("att2", LegacyType.NUMERICAL);

			builder.addInt("att3", i -> i);

			builder.addInt("att4", i -> i);
			builder.addMetaData("att4", LegacyType.NUMERICAL);

			builder.addDateTime("att5", i -> Instant.EPOCH);

			builder.addDateTime("att6", i -> Instant.EPOCH);
			builder.addMetaData("att6", LegacyType.DATE);

			builder.addDateTime("att6.5", i -> Instant.EPOCH);
			builder.addMetaData("att6.5", LegacyType.TIME);

			builder.addTime("att7", i -> LocalTime.NOON);

			builder.addTime("att7.5", i -> LocalTime.NOON);
			builder.addMetaData("att7.5", LegacyType.NUMERICAL);

			builder.addNominal("att8", i -> i % 2 == 0 ? "A" : "B");

			builder.addNominal("att9", i -> i % 2 == 0 ? "A" : "B", 2);

			builder.addNominal("att10", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att10", LegacyType.BINOMINAL);

			builder.addNominal("att11", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att11", LegacyType.STRING);

			builder.addNominal("att12", i -> i % 2 == 0 ? "A" : "B");
			builder.addMetaData("att12", LegacyType.FILE_PATH);

			builder.addNominal("att13", i -> i % 2 == 0 ? "A" : "B", 2);
			builder.addMetaData("att13", LegacyType.NOMINAL);

			builder.addBoolean("att14", i -> i % 2 == 0 ? "A" : "B", "A", ColumnTypes.NOMINAL);

			Table table = builder.build(Belt.defaultContext());

			ExampleSet set = BeltConverter.convertSequentially(new IOTable(table));

			int[] result =
					StreamSupport.stream(set.getAttributes().spliterator(), false).mapToInt(Attribute::getValueType)
							.toArray();
			int[] expected = new int[]{Ontology.REAL, Ontology.NUMERICAL, Ontology.INTEGER, Ontology.NUMERICAL,
					Ontology.DATE_TIME, Ontology.DATE, Ontology.TIME, Ontology.INTEGER, Ontology.NUMERICAL, Ontology.POLYNOMINAL, Ontology.POLYNOMINAL,
					Ontology.BINOMINAL,	Ontology.STRING, Ontology.FILE_PATH, Ontology.NOMINAL, Ontology.BINOMINAL};

			assertArrayEquals(expected, result);
		}

		@Test
		public void testAnnotations() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt("int", i -> 5 * i)
					.build(Belt.defaultContext());

			IOTable tableObject = new IOTable(table);
			tableObject.getAnnotations().setAnnotation(Annotations.KEY_DC_AUTHOR, "gmeier");

			ExampleSet set = BeltConverter.convertSequentially(tableObject);

			assertEquals(tableObject.getAnnotations(), set.getAnnotations());
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
}
