/**
 * Copyright (C) 2001-2018 by RapidMiner and the contributors
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
package com.rapidminer.belt;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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

import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.core.concurrency.ExecutionStoppedException;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.Statistics;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.test.asserter.AsserterFactoryRapidMiner;
import com.rapidminer.test_utils.RapidAssert;
import com.rapidminer.tools.Ontology;


/**
 * Tests the {@link TableViewCreator}.
 *
 * @author Gisa Meier
 */
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

	@BeforeClass
	public static void setup() {
		RapidAssert.ASSERTER_REGISTRY.registerAllAsserters(new AsserterFactoryRapidMiner());
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
		ExampleSet view = TableViewCreator.INSTANCE.createView(table);

		//cannot use RapidAssert since mappings are different
		assertArrayEquals(readExampleSetToStringArray(set), readExampleSetToStringArray(view));
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

		Table table = BeltConverter.convert(set, CONTEXT).getTable();

		RapidAssert.assertEquals(set, TableViewCreator.INSTANCE.createView(table));
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
						i -> Math.random() > 0.7 ? Double.NaN : Math.floor(Math.random() * 60 * 60 * 24 * 1000))
				.withRole(numeric, Attributes.LABEL_NAME)
				.build();

		Table table = BeltConverter.convert(set, CONTEXT).getTable();

		RapidAssert.assertEquals(set, TableViewCreator.INSTANCE.createView(table));
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

		Table table = BeltConverter.convert(set, CONTEXT).getTable();

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
		Table table = BeltConverter.convert(set, CONTEXT).getTable();

		ExampleSet view = TableViewCreator.INSTANCE.createView(table);
		view.getExample(4).setValue(numeric, 5);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testExampleWriteUnsupportedDate() {
		Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
		ExampleSet set = ExampleSets.from(dateTime).withBlankSize(15)
				.withColumnFiller(dateTime,
						i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
				.build();
		Table table = BeltConverter.convert(set, CONTEXT).getTable();

		ExampleSet view = TableViewCreator.INSTANCE.createView(table);
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
						i -> Math.random() > 0.7 ? Double.NaN : Math.floor(Math.random() * 60 * 60 * 24 * 1000))
				.withRole(numeric, Attributes.LABEL_NAME)
				.build();

		Table table = BeltConverter.convert(set, CONTEXT).getTable();
		set.recalculateAllAttributeStatistics();

		ExampleSet view = TableViewCreator.INSTANCE.createView(table);
		view.recalculateAllAttributeStatistics();

		List<Double> setStatistics = new ArrayList<>();
		for(Attribute att: set.getAttributes()){
			if(att.isNumerical()){
				setStatistics.add(set.getStatistics(att, Statistics.AVERAGE));
			}
			setStatistics.add(set.getStatistics(att, Statistics.UNKNOWN));
			setStatistics.add(set.getStatistics(att, Statistics.MINIMUM));
			setStatistics.add(set.getStatistics(att, Statistics.MAXIMUM));
		}

		List<Double> viewStatistics = new ArrayList<>();
		for(Attribute att: view.getAttributes()){
			if(att.isNumerical()){
				viewStatistics.add(view.getStatistics(att, Statistics.AVERAGE));
			}
			viewStatistics.add(view.getStatistics(att, Statistics.UNKNOWN));
			viewStatistics.add(view.getStatistics(att, Statistics.MINIMUM));
			viewStatistics.add(view.getStatistics(att, Statistics.MAXIMUM));
		}

		assertArrayEquals(setStatistics.toArray(), viewStatistics.toArray());
	}

	@Test
	public void testStatisticsWithWeight()  {
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

		Table table = BeltConverter.convert(set, CONTEXT).getTable();
		set.recalculateAllAttributeStatistics();

		ExampleSet view = TableViewCreator.INSTANCE.createView(table);
		view.recalculateAllAttributeStatistics();

		List<Double> setStatistics = new ArrayList<>();
		for(Attribute att: set.getAttributes()){
			if(att.isNumerical()){
				setStatistics.add(set.getStatistics(att, Statistics.AVERAGE));
			}
			setStatistics.add(set.getStatistics(att, Statistics.UNKNOWN));
			setStatistics.add(set.getStatistics(att, Statistics.MINIMUM));
			setStatistics.add(set.getStatistics(att, Statistics.MAXIMUM));
		}

		List<Double> viewStatistics = new ArrayList<>();
		for(Attribute att: view.getAttributes()){
			if(att.isNumerical()){
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

		Table table = BeltConverter.convert(set, CONTEXT).getTable();
		byte[] serialized = serialize(TableViewCreator.INSTANCE.createView(table));
		Object deserialized = deserialize(serialized);

		RapidAssert.assertEquals(set, (ExampleSet) deserialized);
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
						i -> Math.random() > 0.7 ? Double.NaN : Math.floor(Math.random() * 60 * 60 * 24 * 1000))
				.withRole(numeric, Attributes.LABEL_NAME)
				.build();

		Table table = BeltConverter.convert(set, CONTEXT).getTable();
		byte[] serialized = serialize(TableViewCreator.INSTANCE.createView(table));
		Object deserialized = deserialize(serialized);

		RapidAssert.assertEquals(set, (ExampleSet) deserialized);
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

	@Test
	public void testClone() {
		Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
		ExampleSet set = ExampleSets.from(numeric).withBlankSize(15)
				.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random()).build();
		Table table = BeltConverter.convert(set, CONTEXT).getTable();

		ExampleSet view = TableViewCreator.INSTANCE.createView(table);
		ExampleSet clone = (ExampleSet) view.clone();
		RapidAssert.assertEquals(view, clone);
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
	}
}
