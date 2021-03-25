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

import static com.rapidminer.belt.table.BeltConverterTest.assertAttributeOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.core.concurrency.ExecutionStoppedException;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.Statistics;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.NominalMapping;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.test.asserter.AsserterFactoryRapidMiner;
import com.rapidminer.test_utils.RapidAssert;
import com.rapidminer.tools.Ontology;


/**
 * Tests the {@link com.rapidminer.belt.table.TableViewCreator}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class TableViewCreatorTest {

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

	public static class ExampleSetView {
		@BeforeClass
		public static void setup() {
			RapidAssert.ASSERTER_REGISTRY.registerAllAsserters(new AsserterFactoryRapidMiner());
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
			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table);

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testNumericTypes() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			List<Attribute> attributes = Arrays.asList(numeric, real, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet view = TableViewCreator.INSTANCE.createView(table);
			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
		}

		@Test
		public void testNumericAndDateTypes() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet view = TableViewCreator.INSTANCE.createView(table);
			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
		}

		@Test
		public void testExamplesEqual() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(nominal, numeric, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet view1 = new DoubleTableWrapper(table);
			ExampleSet view2 = new DatetimeTableWrapper(table);
			for (int i = 0; i < table.height(); i++) {
				RapidAssert.assertEquals("test", view1.getExample(i), view2.getExample(i));
				RapidAssert.assertEquals("test2", set.getExample(i), view2.getExample(i));
			}
			RapidAssert
					.assertEquals(view1.getExample(0).getDataRow().getType(), view2.getExample(0).getDataRow().getType());
			RapidAssert
					.assertEquals(view1.getExample(1).getDataRow().toString(), view2.getExample(1).getDataRow().toString
							());
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testExampleWriteUnsupported() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			ExampleSet set = ExampleSets.from(numeric).withBlankSize(15)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random()).build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table);
			view.getExample(4).setValue(numeric, 5);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testExampleWriteUnsupportedDate() {
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			ExampleSet set = ExampleSets.from(dateTime).withBlankSize(15)
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table);
			view.getExample(4).setValue(dateTime, 5);
		}

		@Test
		public void testStatistics() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			set.recalculateAllAttributeStatistics();

			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table);
			view.recalculateAllAttributeStatistics();

			List<Double> setStatistics = new ArrayList<>();
			for (Attribute att : set.getAttributes()) {
				if (att.isNumerical()) {
					setStatistics.add(set.getStatistics(att, Statistics.AVERAGE));
				}
				setStatistics.add(set.getStatistics(att, Statistics.UNKNOWN));
				setStatistics.add(set.getStatistics(att, Statistics.MINIMUM));
				setStatistics.add(set.getStatistics(att, Statistics.MAXIMUM));
			}

			List<Double> viewStatistics = new ArrayList<>();
			for (Attribute att : view.getAttributes()) {
				if (att.isNumerical()) {
					viewStatistics.add(view.getStatistics(att, Statistics.AVERAGE));
				}
				viewStatistics.add(view.getStatistics(att, Statistics.UNKNOWN));
				viewStatistics.add(view.getStatistics(att, Statistics.MINIMUM));
				viewStatistics.add(view.getStatistics(att, Statistics.MAXIMUM));
			}

			assertArrayEquals(setStatistics.toArray(), viewStatistics.toArray());
		}

		@Test
		public void testStatisticsWithWeight() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(nominal, numeric, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withRole(numeric, Attributes.WEIGHT_NAME)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			set.recalculateAllAttributeStatistics();

			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table);
			view.recalculateAllAttributeStatistics();

			List<Double> setStatistics = new ArrayList<>();
			for (Attribute att : set.getAttributes()) {
				if (att.isNumerical()) {
					setStatistics.add(set.getStatistics(att, Statistics.AVERAGE));
				}
				setStatistics.add(set.getStatistics(att, Statistics.UNKNOWN));
				setStatistics.add(set.getStatistics(att, Statistics.MINIMUM));
				setStatistics.add(set.getStatistics(att, Statistics.MAXIMUM));
			}

			List<Double> viewStatistics = new ArrayList<>();
			for (Attribute att : view.getAttributes()) {
				if (att.isNumerical()) {
					viewStatistics.add(view.getStatistics(att, Statistics.AVERAGE));
				}
				viewStatistics.add(view.getStatistics(att, Statistics.UNKNOWN));
				viewStatistics.add(view.getStatistics(att, Statistics.MINIMUM));
				viewStatistics.add(view.getStatistics(att, Statistics.MAXIMUM));
			}

			assertArrayEquals(setStatistics.toArray(), viewStatistics.toArray());
		}

		@Test
		public void testSerialization() throws IOException, ClassNotFoundException {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(nominal, numeric, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			byte[] serialized = serialize(com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table));
			Object deserialized = deserialize(serialized);

			ExampleSet deserializedES = (ExampleSet) deserialized;
			RapidAssert.assertEquals(set, deserializedES);
			assertAttributeOrder(set, deserializedES);
		}


		@Test
		public void testSerializationDate() throws IOException, ClassNotFoundException {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();
			byte[] serialized = serialize(com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table));
			Object deserialized = deserialize(serialized);

			ExampleSet deserializedES = (ExampleSet) deserialized;
			RapidAssert.assertEquals(set, deserializedES);
			assertAttributeOrder(set, deserializedES);
		}

		@Test
		public void testSerializationGaps() throws IOException, ClassNotFoundException {
			NominalBuffer buffer = BufferAccessor.get().newUInt8Buffer(ColumnType.NOMINAL, 21);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(7, null);
			buffer.set(5, null);
			NominalBuffer buffer2 = BufferAccessor.get().newUInt8Buffer(ColumnType.NOMINAL, 21);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + i);
			}
			buffer2.set(3, null);
			buffer2.set(5, null);
			Column column = Columns.removeUnusedDictionaryValues(buffer.toColumn(),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Column column2 = Columns.removeUnusedDictionaryValues(buffer2.toColumn(),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());

			NominalBuffer buffer3 = BufferAccessor.get().newUInt2Buffer(ColumnType.NOMINAL, 21);
			buffer3.set(0, "bla");
			for (int i = 0; i < buffer3.size(); i++) {
				buffer3.set(i, "blup");
			}
			buffer3.set(10, null);
			NominalBuffer buffer4 = BufferAccessor.get().newUInt2Buffer(ColumnType.NOMINAL, 21);
			buffer4.set(0, "bla");
			for (int i = 0; i < buffer.size(); i++) {
				buffer4.set(i, "blup");
			}
			buffer4.set(10, null);

			Column bla = Columns.removeUnusedDictionaryValues(buffer3.toBooleanColumn("bla"),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Column blup = Columns.removeUnusedDictionaryValues(buffer4.toBooleanColumn("blup"),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());


			Table table = Builders.newTableBuilder(21).add("first", column)
					.add("second", column2).add("bla", bla).add("blup", blup)
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table);
			byte[] serialized = serialize(set);
			Object deserialized = deserialize(serialized);

			ExampleSet deserializedES = (ExampleSet) deserialized;
			RapidAssert.assertEquals(set, deserializedES);
			assertAttributeOrder(set, deserializedES);
		}

		@Test
		public void testWrongNegativeIndex() throws IOException, ClassNotFoundException {
			//(value0, value1) with value0 positive
			NominalBuffer buffer = Buffers.nominalBuffer(4, 3);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 2));
			}
			Column column1 = buffer.toBooleanColumn("value0");
			assertEquals(1, column1.getDictionary().getPositiveIndex());

			//(value) with value positive
			NominalBuffer buffer2 = Buffers.nominalBuffer(4, 3);
			for (int i = 0; i < buffer.size(); i++) {
				buffer2.set(i, "value");
			}
			Column column2 = buffer2.toBooleanColumn("value");
			assertEquals(1, column2.getDictionary().getPositiveIndex());
			assertFalse(column2.getDictionary().hasNegative());

			//() with no positive
			Column column3 = Buffers.<String>nominalBuffer(4, 1).toBooleanColumn(null);

			//(value0, value1) with value1 positive and value0 unused
			NominalBuffer buffer4 = Buffers.nominalBuffer(4, 3);
			for (int i = 0; i < buffer4.size(); i++) {
				buffer4.set(i, "value" + (i % 2));
			}
			buffer4.set(0, null);
			buffer4.set(2, null);
			Column column4 = buffer4.toBooleanColumn("value1");
			assertEquals(1, column4.getDictionary().getNegativeIndex());
			Column column4new = Columns.removeUnusedDictionaryValues(column4, Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Column column5 = Columns.removeUnusedDictionaryValues(column4, Columns.CleanupOption.COMPACT, Belt.defaultContext());

			Table table = new Table(new Column[]{column1, column2, column3, column4new, column5}, new String[]{"a", "b", "c", "d", "e"});
			Table adjustedTable = TableViewCreator.INSTANCE.adjustDictionaries(table);

			assertEquals(1, adjustedTable.column(0).getDictionary().getNegativeIndex());
			assertEquals(1, adjustedTable.column(1).getDictionary().getNegativeIndex());
			assertEquals(1, adjustedTable.column(3).getDictionary().getNegativeIndex());
			assertEquals(1, adjustedTable.column(4).getDictionary().getNegativeIndex());
			assertEquals(0, adjustedTable.column(2).getDictionary().size());
			assertEquals(table.toString(), adjustedTable.toString());
		}

		@Test
		public void testClone() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			ExampleSet set = ExampleSets.from(numeric).withBlankSize(15)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random()).build();
			Table table = com.rapidminer.belt.table.BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.createView(table);
			ExampleSet clone = (ExampleSet) view.clone();
			RapidAssert.assertEquals(view, clone);
			assertAttributeOrder(view, clone);
		}

		@Test
		public void testCloneDate() {
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			ExampleSet set = ExampleSets.from(dateTime).withBlankSize(15)
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.build();
			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet view = TableViewCreator.INSTANCE.createView(table);
			ExampleSet clone = (ExampleSet) view.clone();
			RapidAssert.assertEquals(view, clone);
			assertAttributeOrder(view, clone);
		}

		@Test(expected = BeltConverter.ConversionException.class)
		public void testAdvancedColumns() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.addTextset("textset", i -> new StringSet(Collections.singleton("val" + i)))
					.build(Belt.defaultContext());
			TableViewCreator.INSTANCE.createView(table);
		}

		@Test
		public void testReplaceAdvancedColumns() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0)
					.addTextset("textset", i -> new StringSet(Collections.singleton("val" + i))).addInt53Bit("int", i -> 5 * i)
					.build(Belt.defaultContext());
			Table replaced = TableViewCreator.INSTANCE.replacedAdvancedWithError(table);
			double[] first = new double[11];
			table.column("real").fill(first, 0);
			double[] third = new double[11];
			table.column("int").fill(third, 0);
			double[] constant = new double[11];
			Arrays.fill(constant, 1);
			assertArrayEquals(new double[][]{first, constant, third},
					BeltConverterTest.readTableToArray(replaced));
			Object[] message = new Object[1];
			replaced.column("textset").fill(message, 0);
			assertEquals("Error: Cannot display advanced column of Column type Text-Set", message[0]);
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

			Table table = BeltConverter.convert(set, CONTEXT).getTable();

			ExampleSet backSet = TableViewCreator.INSTANCE.createView(table);
			RapidAssert.assertEquals(set, backSet);

			assertAttributeOrder(set, backSet);
		}
	}

	public static class ExampleTableView {

		@BeforeClass
		public static void setup() {
			RapidAssert.ASSERTER_REGISTRY.registerAllAsserters(new AsserterFactoryRapidMiner());
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

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
		}

		@Test
		public void testNumericTypes() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			List<Attribute> attributes = Arrays.asList(numeric, real, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
		}

		@Test
		public void testNumericAndDateTypes() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
		}

		@Test
		public void testExamplesEqual() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(nominal, numeric, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);

			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			for (int i = 0; i < view.size(); i++) {
				RapidAssert.assertEquals("test", set.getExample(i), view.getExample(i));
			}
			RapidAssert
					.assertEquals(view.getExample(0).getDataRow().getType(), set.getExample(0).getDataRow().getType());
			RapidAssert
					.assertEquals(view.getExample(1).toString(), set.getExample(1).toString());
			RapidAssert
					.assertEquals(view.iterator().next().getDataRow().getType(), set.iterator().next().getDataRow().getType());
		}


		@Test
		public void testStatistics() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			set.recalculateAllAttributeStatistics();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			view.recalculateAllAttributeStatistics();

			List<Double> setStatistics = new ArrayList<>();
			for (Attribute att : set.getAttributes()) {
				if (att.isNumerical()) {
					setStatistics.add(set.getStatistics(att, Statistics.AVERAGE));
				}
				setStatistics.add(set.getStatistics(att, Statistics.UNKNOWN));
				setStatistics.add(set.getStatistics(att, Statistics.MINIMUM));
				setStatistics.add(set.getStatistics(att, Statistics.MAXIMUM));
			}

			List<Double> viewStatistics = new ArrayList<>();
			for (Attribute att : view.getAttributes()) {
				if (att.isNumerical()) {
					viewStatistics.add(view.getStatistics(att, Statistics.AVERAGE));
				}
				viewStatistics.add(view.getStatistics(att, Statistics.UNKNOWN));
				viewStatistics.add(view.getStatistics(att, Statistics.MINIMUM));
				viewStatistics.add(view.getStatistics(att, Statistics.MAXIMUM));
			}

			assertArrayEquals(setStatistics.toArray(), viewStatistics.toArray());
		}

		@Test
		public void testStatisticsWithWeight() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(nominal, numeric, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withRole(numeric, Attributes.WEIGHT_NAME)
					.build();

			set.recalculateAllAttributeStatistics();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			view.recalculateAllAttributeStatistics();

			List<Double> setStatistics = new ArrayList<>();
			for (Attribute att : set.getAttributes()) {
				if (att.isNumerical()) {
					setStatistics.add(set.getStatistics(att, Statistics.AVERAGE));
				}
				setStatistics.add(set.getStatistics(att, Statistics.UNKNOWN));
				setStatistics.add(set.getStatistics(att, Statistics.MINIMUM));
				setStatistics.add(set.getStatistics(att, Statistics.MAXIMUM));
			}

			List<Double> viewStatistics = new ArrayList<>();
			for (Attribute att : view.getAttributes()) {
				if (att.isNumerical()) {
					viewStatistics.add(view.getStatistics(att, Statistics.AVERAGE));
				}
				viewStatistics.add(view.getStatistics(att, Statistics.UNKNOWN));
				viewStatistics.add(view.getStatistics(att, Statistics.MINIMUM));
				viewStatistics.add(view.getStatistics(att, Statistics.MAXIMUM));
			}

			assertArrayEquals(setStatistics.toArray(), viewStatistics.toArray());
		}

		@Test
		public void testSerialization() throws IOException, ClassNotFoundException {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(nominal, numeric, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			byte[] serialized = serialize(TableViewCreator.INSTANCE.convertOnWriteView(table, true));
			Object deserialized = deserialize(serialized);

			ExampleSet deserializedES = (ExampleSet) deserialized;
			RapidAssert.assertEquals(set, deserializedES);
			assertAttributeOrder(set, deserializedES);
		}


		@Test
		public void testSerializationDate() throws IOException, ClassNotFoundException {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			byte[] serialized = serialize(TableViewCreator.INSTANCE.convertOnWriteView(table, true));
			Object deserialized = deserialize(serialized);

			ExampleSet deserializedES = (ExampleSet) deserialized;
			RapidAssert.assertEquals(set, deserializedES);
			assertAttributeOrder(set, deserializedES);
		}

		@Test
		public void testSerializationGaps() throws IOException, ClassNotFoundException {
			NominalBuffer buffer = BufferAccessor.get().newUInt8Buffer(ColumnType.NOMINAL, 21);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(7, null);
			buffer.set(5, null);
			NominalBuffer buffer2 = BufferAccessor.get().newUInt8Buffer(ColumnType.NOMINAL, 21);
			for (int i = 0; i < buffer2.size(); i++) {
				buffer2.set(i, "val" + i);
			}
			buffer2.set(3, null);
			buffer2.set(5, null);
			Column column = Columns.removeUnusedDictionaryValues(buffer.toColumn(),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Column column2 = Columns.removeUnusedDictionaryValues(buffer2.toColumn(),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());

			NominalBuffer buffer3 = BufferAccessor.get().newUInt2Buffer(ColumnType.NOMINAL, 21);
			buffer3.set(0, "bla");
			for (int i = 0; i < buffer3.size(); i++) {
				buffer3.set(i, "blup");
			}
			buffer3.set(10, null);
			NominalBuffer buffer4 = BufferAccessor.get().newUInt2Buffer(ColumnType.NOMINAL, 21);
			buffer4.set(0, "bla");
			for (int i = 0; i < buffer.size(); i++) {
				buffer4.set(i, "blup");
			}
			buffer4.set(10, null);

			Column bla = Columns.removeUnusedDictionaryValues(buffer3.toBooleanColumn("bla"),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());
			Column blup = Columns.removeUnusedDictionaryValues(buffer4.toBooleanColumn("blup"),
					Columns.CleanupOption.REMOVE, Belt.defaultContext());


			Table table = Builders.newTableBuilder(21).add("first", column)
					.add("second", column2).add("bla", bla).add("blup", blup)
					.build(Belt.defaultContext());

			ExampleSet set = com.rapidminer.belt.table.TableViewCreator.INSTANCE.convertOnWriteView(new IOTable(table), true);
			byte[] serialized = serialize(set);
			Object deserialized = deserialize(serialized);

			ExampleSet deserializedES = (ExampleSet) deserialized;
			RapidAssert.assertEquals(set, deserializedES);
			assertAttributeOrder(set, deserializedES);
		}

		@Test
		public void testClone() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			ExampleSet set = ExampleSets.from(numeric).withBlankSize(15)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random()).build();
			IOTable table = BeltConverter.convert(set, CONTEXT);

			ExampleSet view = com.rapidminer.belt.table.TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			ExampleSet clone = (ExampleSet) view.clone();
			RapidAssert.assertEquals(view, clone);
			assertAttributeOrder(view, clone);
		}

		@Test
		public void testCloneDate() {
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			ExampleSet set = ExampleSets.from(dateTime).withBlankSize(15)
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.build();
			IOTable table = BeltConverter.convert(set, CONTEXT);

			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, false);
			ExampleSet clone = (ExampleSet) view.clone();
			RapidAssert.assertEquals(view, clone);
			assertAttributeOrder(view, clone);
		}

		@Test(expected = BeltConverter.ConversionException.class)
		public void testAdvancedColumns() {
			Table table = Builders.newTableBuilder(11).addReal("real", i -> 3 * i / 5.0).addInt53Bit("int", i -> 5 * i)
					.addTextset("textset", i -> new StringSet(Collections.singleton("val" + i)))
					.build(Belt.defaultContext());
			TableViewCreator.INSTANCE.convertOnWriteView(new IOTable(table), true);
		}

		@Test
		public void testReplaceAdvancedColumns() {
			int size = 11;
			Table table = Builders.newTableBuilder(size).addReal("real", i -> 3 * i / 5.0)
					.addTextset("textset", i -> new StringSet(Collections.singleton("val" + i))).addInt53Bit("int", i -> 5 * i)
					.build(Belt.defaultContext());
			ExampleSet replaced = TableViewCreator.INSTANCE.convertOnWriteView(new IOTable(table), false);
			double[] first = new double[size];
			table.column("real").fill(first, 0);
			double[] third = new double[size];
			table.column("int").fill(third, 0);
			double[] constant = new double[size];
			Arrays.fill(constant, 0);
			assertArrayEquals(new double[][]{first, constant, third},
					BeltConverterTest.readExampleSetToArray(replaced));
			NominalMapping mapping = replaced.getAttributes().get("textset").getMapping();
			assertEquals(1, mapping.size());
			assertEquals("Cannot display advanced column", mapping.mapIndex(0));
		}

		@Test
		public void testColumnCleanupNoDate() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute nominal = AttributeFactory.createAttribute("nominal", Ontology.NOMINAL);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			for (int i = 0; i < 5; i++) {
				nominal.getMapping().mapString("nominalValue" + i);
			}
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(nominal, numeric, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withRole(numeric, Attributes.WEIGHT_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attributes setAttributes = set.getAttributes();
			setAttributes.remove(real);
			setAttributes.remove(polynominal);
			set.cleanup();

			Attributes viewAttributes = view.getAttributes();
			viewAttributes.remove(viewAttributes.findRoleByName(real.getName()));
			viewAttributes.remove(viewAttributes.findRoleByName(polynominal.getName()));
			view.cleanup();

			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
		}

		@Test
		public void testColumnCleanupWithDate() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attributes setAttributes = set.getAttributes();
			setAttributes.remove(real);
			setAttributes.remove(date);
			set.cleanup();

			Attributes viewAttributes = view.getAttributes();
			viewAttributes.remove(viewAttributes.findRoleByName(real.getName()));
			viewAttributes.remove(viewAttributes.findRoleByName(date.getName()));
			view.cleanup();

			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
		}

		@Test
		public void testColumnCleanupWithDateNoDateAfter() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			List<Attribute> attributes = Arrays.asList(numeric, real, dateTime, date, time, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : BeltConverterTest.randomTimeMillis())
					.withRole(numeric, Attributes.LABEL_NAME)
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attributes setAttributes = set.getAttributes();
			setAttributes.remove(dateTime);
			setAttributes.remove(date);
			setAttributes.remove(time);
			set.cleanup();

			Attributes viewAttributes = view.getAttributes();
			viewAttributes.remove(viewAttributes.findRoleByName(dateTime.getName()));
			viewAttributes.remove(viewAttributes.findRoleByName(date.getName()));
			viewAttributes.remove(viewAttributes.findRoleByName(time.getName()));
			view.cleanup();

			RapidAssert.assertEquals(set, view);
			assertAttributeOrder(set, view);
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

			IOTable table = BeltConverter.convert(set, CONTEXT);

			ExampleSet backSet = TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			RapidAssert.assertEquals(set, backSet);

			assertAttributeOrder(set, backSet);
		}
	}

	public static class GapRemoval {

		@Test
		public void testGapRemoval(){
			final NominalBuffer nominalBuffer = Buffers.nominalBuffer(100);
			nominalBuffer.set(0, "red");
			nominalBuffer.set(1, "green");
			for (int i = 1; i <= 50; i++) {
				nominalBuffer.set(i, "blue");
			}
			for (int i = 51; i < 100; i++) {
				nominalBuffer.set(i, "green");
			}
			Table table =
					Builders.newTableBuilder(100).add("nominal", nominalBuffer.toColumn()).add("nominal2",
							nominalBuffer.toColumn()).add("nominal3", nominalBuffer.toColumn()).build(Belt.defaultContext());
			table = table.rows(0, 50, Belt.defaultContext());
			table =
					Builders.newTableBuilder(50).add("nominal",
							Columns.removeUnusedDictionaryValues(table.column("nominal"), Columns.CleanupOption.REMOVE,
									Belt.defaultContext())).add("nominal2",
							table.column("nominal2")).add("nominal3",
							Columns.removeUnusedDictionaryValues(table.column("nominal3"), Columns.CleanupOption.COMPACT,
									Belt.defaultContext())).build(Belt.defaultContext());
			final Table compacted = TableViewCreator.INSTANCE.compactDictionaries(table);
			assertNotSame(table, compacted);
			for (Column column : compacted.columnList()) {
				assertEquals(column.getDictionary().maximalIndex(), column.getDictionary().size());
			}
		}

		@Test
		public void testNoGapRemoval(){
			final NominalBuffer nominalBuffer = Buffers.nominalBuffer(100);
			nominalBuffer.set(0, "red");
			nominalBuffer.set(1, "green");
			for (int i = 1; i <= 50; i++) {
				nominalBuffer.set(i, "blue");
			}
			for (int i = 51; i < 100; i++) {
				nominalBuffer.set(i, "green");
			}
			Table table =
					Builders.newTableBuilder(100).add("nominal", nominalBuffer.toColumn()).add("nominal2",
							nominalBuffer.toColumn()).add("nominal3", nominalBuffer.toColumn()).build(Belt.defaultContext());
			table = table.rows(0, 50, Belt.defaultContext());
			final Table compacted = TableViewCreator.INSTANCE.compactDictionaries(table);
			assertSame(table, compacted);
			for (Column column : compacted.columnList()) {
				assertEquals(column.getDictionary().maximalIndex(), column.getDictionary().size());
			}
		}
	}


	private static byte[] serialize(Object obj) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream o = new ObjectOutputStream(b);
		o.writeObject(obj);
		return b.toByteArray();
	}

	private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream b = new ByteArrayInputStream(bytes);
		ObjectInputStream o = new ObjectInputStream(b);
		return o.readObject();
	}
}