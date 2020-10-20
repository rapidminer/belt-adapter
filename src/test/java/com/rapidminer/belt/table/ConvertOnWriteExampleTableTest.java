/**
 * Copyright (C) 2001-2020 by RapidMiner and the contributors
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.core.concurrency.ExecutionStoppedException;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.ExampleTable;
import com.rapidminer.example.utils.ExampleSets;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.preprocessing.MaterializeDataInMemory;
import com.rapidminer.test.asserter.AsserterFactoryRapidMiner;
import com.rapidminer.test_utils.RapidAssert;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.att.AttributeSet;


/**
 * Tests the {@link ConvertOnWriteExampleTable}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ConvertOnWriteExampleTableTest {

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

	@RunWith(Parameterized.class)
	@FixMethodOrder(MethodSorters.NAME_ASCENDING) //want the get methods first and add methods last
	public static class TableMethods {

		@BeforeClass
		public static void setup() {
			RapidAssert.ASSERTER_REGISTRY.registerAllAsserters(new AsserterFactoryRapidMiner());
		}

		@Parameterized.Parameter
		public ExampleTable comparisonTable;

		@Parameterized.Parameter(value = 1)
		public ConvertOnWriteExampleTable testTable;

		@Parameterized.Parameters
		public static Collection<Object[]> params() {
			List<Object[]> params = new ArrayList<>();
			ExampleSet set = getNumericExampleSet();
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(BeltConverter.convert(set, CONTEXT), true);
			// only wrapped table
			params.add(new Object[]{set.getExampleTable(), (ConvertOnWriteExampleTable)view.getExampleTable()});

			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			set = getNumericExampleSet();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute numericClone = (Attribute) numeric.clone();
			view.getExampleTable().addAttribute(numericClone);
			view.getAttributes().addRegular(numericClone);
			int i = 0;
			for (Example example : view) {
				example.setValue(numericClone, 0.42 * (i++));
			}

			set.getExampleTable().addAttribute(numeric);
			set.getAttributes().addRegular(numeric);
			i = 0;
			for (Example example : set) {
				example.setValue(numeric, 0.42 * (i++));
			}
			// with added columns
			params.add(new Object[]{set.getExampleTable(), (ConvertOnWriteExampleTable)view.getExampleTable()});

			set = getNumericExampleSet();
			Attribute real = set.getAttributes().get("real");

			table = BeltConverter.convert(set, CONTEXT);
			view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			Attribute realView = view.getAttributes().get(real.getName());
			view.getAttributes().remove(realView);
			view.getExampleTable().removeAttribute(realView);

			set.getAttributes().remove(real);
			set.getExampleTable().removeAttribute(real);

			// with converted
			params.add(new Object[]{set.getExampleTable(), (ConvertOnWriteExampleTable)view.getExampleTable()});
			return params;
		}

		@Test
		public void a1testGetAttributes() {
			RapidAssert.assertArrayEquals(comparisonTable.getAttributes(), testTable.getAttributes());
			Object[] objects = new Object[4];
			Object[] expectedObjects = new Object[4];
			Arrays.setAll(objects, i-> testTable.getAttribute(i));
			Arrays.setAll(expectedObjects, i-> comparisonTable.getAttribute(i));
			RapidAssert.assertArrayEquals(expectedObjects, objects);
		}

		@Test
		public void a2testFindAttributes() {
			String[] labels = new String[]{"real", "integer", "numeric", "dateTime", "date", "time", "buntekuh", null};
			Object[] objects = new Object[labels.length];
			Object[] expectedObjects = new Object[labels.length];
			Arrays.setAll(objects, i-> {
				try {
					return testTable.findAttribute(labels[i]);
				} catch (OperatorException e) {
					return null;
				}
			});
			Arrays.setAll(expectedObjects, i-> {
				try {
					return comparisonTable.findAttribute(labels[i]);
				} catch (OperatorException e) {
					return null;
				}
			});
			RapidAssert.assertArrayEquals(expectedObjects, objects);
		}

		@Test
		public void testGetAttributeNumbers() {
			assertEquals(comparisonTable.getAttributeCount(), testTable.getAttributeCount());
			assertEquals(comparisonTable.getNumberOfAttributes(), testTable.getNumberOfAttributes());
		}

		@Test
		public void testCreateExampleSet() {
			RapidAssert.assertEquals(comparisonTable.createExampleSet(comparisonTable.getAttribute(0)),
					testTable.createExampleSet(testTable.getAttribute(0)));
		}

		@Test
		public void testCreateExampleSet1() {
			AttributeRole label = new AttributeRole(comparisonTable.getAttribute(2));
			label.setSpecial(Attributes.LABEL_NAME);
			List<AttributeRole> comparisonList = Arrays.asList(new AttributeRole(comparisonTable.getAttribute(3)), label,
					new AttributeRole(comparisonTable.getAttribute(1)));
			AttributeRole testLabel = new AttributeRole(testTable.getAttribute(2));
			testLabel.setSpecial(Attributes.LABEL_NAME);
			List<AttributeRole> testList = Arrays.asList(new AttributeRole(testTable.getAttribute(3)), testLabel,
					new AttributeRole(testTable.getAttribute(1)));

			RapidAssert.assertEquals(comparisonTable.createExampleSet(comparisonList.iterator()),
					testTable.createExampleSet(testList.iterator()));
		}

		@Test
		public void testCreateExampleSet2() {
			RapidAssert.assertEquals(comparisonTable.createExampleSet(comparisonTable.getAttribute(0), comparisonTable.getAttribute(1), comparisonTable.getAttribute(2)),
					testTable.createExampleSet(testTable.getAttribute(0), testTable.getAttribute(1), testTable.getAttribute(2)));
		}

		@Test
		public void testCreateExampleSet3() {
			Map<String, Attribute> comparisonMap = new HashMap<>();
			comparisonMap.put(Attributes.LABEL_NAME, comparisonTable.getAttribute(2));
			comparisonMap.put("blablup", comparisonTable.getAttribute(3));
			AttributeSet comparisonSet = new AttributeSet(Arrays.asList(comparisonTable.getAttribute(0), comparisonTable.getAttribute(1)),comparisonMap);

			Map<String, Attribute> testMap = new HashMap<>();
			testMap.put(Attributes.LABEL_NAME, testTable.getAttribute(2));
			testMap.put("blablup", testTable.getAttribute(3));
			AttributeSet testSet = new AttributeSet(Arrays.asList(testTable.getAttribute(0), testTable.getAttribute(1)),testMap);

			RapidAssert.assertEquals(comparisonTable.createExampleSet(comparisonSet),
					testTable.createExampleSet(testSet));
		}

		@Test
		public void testToString() {
			assertEquals(comparisonTable.toString(), testTable.toString());
			assertEquals(comparisonTable.toDataString(), testTable.toDataString());
		}

		@Test
		public void testRemoveAdddedAttribute() {
			Attribute comparisonAttribute = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
			comparisonTable.addAttribute(comparisonAttribute);
			Attribute testAttribute = (Attribute) comparisonAttribute.clone();
			testTable.addAttribute(testAttribute);
			comparisonTable.removeAttribute(comparisonAttribute.getTableIndex());
			testTable.removeAttribute(testAttribute.getTableIndex());
			RapidAssert.assertArrayEquals(comparisonTable.getAttributes(), testTable.getAttributes());
		}

		@Test
		public void z1testAddAttribute() {
			Attribute comparisonAttribute = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
			comparisonTable.addAttribute(comparisonAttribute);
			testTable.addAttribute((Attribute) comparisonAttribute.clone());
			RapidAssert.assertArrayEquals(comparisonTable.getAttributes(), testTable.getAttributes());
		}

		@Test
		public void z2testAddAttributes() {
			Attribute comparisonAttribute = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
			Attribute comparisonAttribute2 = AttributeFactory.createAttribute("test2", Ontology.NUMERICAL);
			comparisonTable.addAttributes(Arrays.asList(comparisonAttribute, comparisonAttribute2));
			testTable.addAttributes(Arrays.asList((Attribute) comparisonAttribute.clone(), (Attribute) comparisonAttribute2.clone()));
			RapidAssert.assertArrayEquals(comparisonTable.getAttributes(), testTable.getAttributes());
		}

	}

	public static class ExampleSetMethods {

		@BeforeClass
		public static void setup() {
			RapidAssert.ASSERTER_REGISTRY.registerAllAsserters(new AsserterFactoryRapidMiner());
		}

		@Test
		public void testSetNumeric() {
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

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute realInView = view.getAttributes().get(real.getName());
			for (Example example : view) {
				example.setValue(realInView, 0.42);
			}

			for (Example example : set) {
				example.setValue(real, 0.42);
			}

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testSetNominal() {
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
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			Attribute polynominalInView = view.getAttributes().get(polynominal.getName());
			view.getExample(42).setValue(polynominalInView, "hello");

			set.getExample(42).setValue(polynominal, "hello");

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testAddAndSet() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			List<Attribute> attributes = Arrays.asList(real, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute numericClone = (Attribute) numeric.clone();
			view.getExampleTable().addAttribute(numericClone);
			view.getAttributes().addRegular(numericClone);
			int i = 0;
			for (Example example : view) {
				example.setValue(numericClone, 0.42 * (i++));
			}
			view.getExample(42).setValue(numericClone, Double.NaN);

			set.getExampleTable().addAttribute(numeric);
			set.getAttributes().addRegular(numeric);
			i = 0;
			for (Example example : set) {
				example.setValue(numeric, 0.42 * (i++));
			}
			set.getExample(42).setValue(numeric, Double.NaN);

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testAddAndSetAndCleanup() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			List<Attribute> attributes = Arrays.asList(real, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute numericClone = (Attribute) numeric.clone();
			view.getExampleTable().addAttribute(numericClone);
			view.getAttributes().addRegular(numericClone);
			int i = 0;
			for (Example example : view) {
				example.setValue(numericClone, 0.42 * (i++));
			}
			view.getAttributes().remove(view.getAttributes().get(integer.getName()));
			view.cleanup();

			set.getExampleTable().addAttribute(numeric);
			set.getAttributes().addRegular(numeric);
			i = 0;
			for (Example example : set) {
				example.setValue(numeric, 0.42 * (i++));
			}
			set.getAttributes().remove(integer);
			set.cleanup();

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testAddAndSetAndConvertAndCleanup() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			List<Attribute> attributes = Arrays.asList(real, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute numericClone = (Attribute) numeric.clone();
			view.getExampleTable().addAttribute(numericClone);
			view.getAttributes().addRegular(numericClone);
			int i = 0;
			for (Example example : view) {
				example.setValue(numericClone, 0.42 * (i++));
			}
			view.getExample(42).setValue(view.getAttributes().get(real.getName()), 42);

			view.getAttributes().remove(view.getAttributes().get(integer.getName()));
			view.cleanup();

			set.getExampleTable().addAttribute(numeric);
			set.getAttributes().addRegular(numeric);
			i = 0;
			for (Example example : set) {
				example.setValue(numeric, 0.42 * (i++));
			}
			set.getExample(42).setValue(real, 42);

			set.getAttributes().remove(integer);
			set.cleanup();

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testAddAndSetAndMaterialize() {
			Random random = new Random();
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(real, date, integer, time, polynominal, dateTime, binominal);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : Math.floor(Math.random() * 60 * 60 * 24 * 1000))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute numericClone = (Attribute) numeric.clone();
			view.getExampleTable().addAttribute(numericClone);
			view.getAttributes().addRegular(numericClone);
			int i = 0;
			for (Example example : view) {
				example.setValue(numericClone, 0.42 * (i++));
			}

			set.getExampleTable().addAttribute(numeric);
			set.getAttributes().addRegular(numeric);
			i = 0;
			for (Example example : set) {
				example.setValue(numeric, 0.42 * (i++));
			}

			RapidAssert.assertEquals(set, MaterializeDataInMemory.materializeExampleSet(view));
		}

		@Test
		public void testAddAndSetAndRemove() {
			Attribute numeric = AttributeFactory.createAttribute("numeric", Ontology.NUMERICAL);
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			List<Attribute> attributes = Arrays.asList(real, integer);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute numericClone = (Attribute) numeric.clone();
			view.getExampleTable().addAttribute(numericClone);
			view.getAttributes().addRegular(numericClone);
			int i = 0;
			for (Example example : view) {
				example.setValue(numericClone, 0.42 * (i++));
			}
			view.getAttributes().remove(numericClone);
			view.getExampleTable().removeAttribute(numericClone);

			set.getExampleTable().addAttribute(numeric);
			set.getAttributes().addRegular(numeric);
			i = 0;
			for (Example example : set) {
				example.setValue(numeric, 0.42 * (i++));
			}
			set.getAttributes().remove(numeric);
			set.getExampleTable().removeAttribute(numeric);

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testRemoveFromExisting() {
			Random random = new Random();
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(real, date, integer, time, polynominal, dateTime, binominal);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : Math.floor(Math.random() * 60 * 60 * 24 * 1000))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attributes viewAttributes = view.getAttributes();
			Attribute integerInView = viewAttributes.get(integer.getName());
			viewAttributes.remove(integerInView);
			Attribute binominalInView = viewAttributes.get(binominal.getName());
			viewAttributes.remove(binominalInView);
			Attribute dateInView = viewAttributes.get(date.getName());
			viewAttributes.remove(dateInView);
			view.getExampleTable().removeAttribute(integerInView);
			view.getExampleTable().removeAttribute(binominalInView);
			view.getExampleTable().removeAttribute(dateInView);

			Attributes setAttributes = set.getAttributes();
			setAttributes.remove(integer);
			setAttributes.remove(binominal);
			setAttributes.remove(date);
			set.getExampleTable().removeAttribute(integer);
			set.getExampleTable().removeAttribute(binominal);
			set.getExampleTable().removeAttribute(date);

			RapidAssert.assertEquals(set, view);
		}

		@Test
		public void testRemoveFromExistingWithTableIndex() {
			Random random = new Random();
			Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
			Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
			Attribute polynominal = AttributeFactory.createAttribute("polynominal", Ontology.POLYNOMINAL);
			Attribute binominal = AttributeFactory.createAttribute("binominal", Ontology.BINOMINAL);
			Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
			Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
			Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
			for (int i = 0; i < 6; i++) {
				polynominal.getMapping().mapString("polyValue" + i);
			}
			for (int i = 0; i < 2; i++) {
				binominal.getMapping().mapString("binominalValue" + i);
			}
			List<Attribute> attributes = Arrays.asList(real, date, integer, time, polynominal, dateTime, binominal);
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.withColumnFiller(dateTime,
							i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
					.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
							230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
					.withColumnFiller(time,
							i -> Math.random() > 0.7 ? Double.NaN : Math.floor(Math.random() * 60 * 60 * 24 * 1000))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attributes viewAttributes = view.getAttributes();
			Attribute integerInView = viewAttributes.get(integer.getName());
			viewAttributes.remove(integerInView);
			Attribute binominalInView = viewAttributes.get(binominal.getName());
			viewAttributes.remove(binominalInView);
			Attribute dateInView = viewAttributes.get(date.getName());
			viewAttributes.remove(dateInView);
			view.getExampleTable().removeAttribute(integerInView.getTableIndex());
			view.getExampleTable().removeAttribute(binominalInView.getTableIndex());
			view.getExampleTable().removeAttribute(dateInView.getTableIndex());

			Attributes setAttributes = set.getAttributes();
			setAttributes.remove(integer);
			setAttributes.remove(binominal);
			setAttributes.remove(date);
			set.getExampleTable().removeAttribute(integer.getTableIndex());
			set.getExampleTable().removeAttribute(binominal.getTableIndex());
			set.getExampleTable().removeAttribute(date.getTableIndex());

			RapidAssert.assertEquals(set, view);
		}


		@Test
		public void testDataRowToString() {
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
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(15)
					.withColumnFiller(numeric, i -> Math.random() > 0.7 ? Double.NaN : Math.random())
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);

			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			Iterator<Example> iterator = view.iterator();
			for (int i = 0; i < view.size(); i++) {
				RapidAssert.assertEquals("test", set.getExample(i).getDataRow().toString(), iterator.next().getDataRow().toString());
			}
			RapidAssert
					.assertEquals(view.getExample(0).getDataRow().toString(), set.getExample(0).getDataRow().toString());
		}

		@Test
		public void testSerializationWithAdditionalColumn() throws IOException, ClassNotFoundException {
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
			List<Attribute> attributes = Arrays.asList(nominal, real, integer, polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
					.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			Attribute numericClone = (Attribute) numeric.clone();
			view.getExampleTable().addAttribute(numericClone);
			view.getAttributes().addRegular(numericClone);
			int i = 0;
			for (Example example : view) {
				example.setValue(numericClone, 0.42 * (i++));
			}

			set.getExampleTable().addAttribute(numeric);
			set.getAttributes().addRegular(numeric);
			i = 0;
			for (Example example : set) {
				example.setValue(numeric, 0.42 * (i++));
			}


			byte[] serialized = serialize(view);
			Object deserialized = deserialize(serialized);

			RapidAssert.assertEquals(set, (ExampleSet) deserialized);
		}

		@Test
		public void testSerializationAfterSetExisting() throws IOException, ClassNotFoundException {
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
			view.getExample(42).setValue(view.getAttributes().get(real.getName()), 42);
			byte[] serialized = serialize(view);
			Object deserialized = deserialize(serialized);

			set.getExample(42).setValue(real, 42);
			RapidAssert.assertEquals(set, (ExampleSet) deserialized);
		}

		@Test
		public void testCleanupAndConvert() {
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

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			set.getAttributes().remove(set.getAttributes().get("real"));
			set.getAttributes().remove(set.getAttributes().get("date"));
			view.getAttributes().remove(view.getAttributes().get("real"));
			view.getAttributes().remove(view.getAttributes().get("date"));
			set.cleanup();
			view.cleanup();
			set.getExampleTable().removeAttribute(set.getAttributes().get("integer"));
			view.getExampleTable().removeAttribute(view.getAttributes().get("integer"));

			RapidAssert.assertEquals(set, view);
			assertEquals(set.getExampleTable().getAttributeCount(), view.getExampleTable().getAttributeCount());
		}

		@Test
		public void testCleanupAndConvertWithAdditional() {
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

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);

			set.getAttributes().remove(set.getAttributes().get("real"));
			Attribute numeric2 = AttributeFactory.createAttribute("numeric2", Ontology.NUMERICAL);
			set.getExampleTable().addAttribute(numeric2);
			set.getAttributes().addRegular(numeric2);
			Attribute integer2 = AttributeFactory.createAttribute("integer2", Ontology.INTEGER);
			set.getExampleTable().addAttribute(integer2);
			set.getAttributes().addRegular(integer2);
			set.getAttributes().remove(numeric2);


			view.getAttributes().remove(view.getAttributes().get("real"));
			numeric2 = AttributeFactory.createAttribute("numeric2", Ontology.NUMERICAL);
			view.getExampleTable().addAttribute(numeric2);
			view.getAttributes().addRegular(numeric2);
			integer2 = AttributeFactory.createAttribute("integer2", Ontology.INTEGER);
			view.getExampleTable().addAttribute(integer2);
			view.getAttributes().addRegular(integer2);
			view.getAttributes().remove(numeric2);

			set.cleanup();
			view.cleanup();

			set.getExampleTable().removeAttribute(set.getAttributes().get("integer"));
			view.getExampleTable().removeAttribute(view.getAttributes().get("integer"));

			RapidAssert.assertEquals(set, view);
			assertEquals(set.getExampleTable().getAttributeCount(), view.getExampleTable().getAttributeCount());
		}

		@Test
		public void testCleanupAndConvertNominal() throws OperatorException {
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
			List<Attribute> attributes = Arrays.asList(nominal,  polynominal, binominal);
			Random random = new Random();
			ExampleSet set = ExampleSets.from(attributes).withBlankSize(150)
					.withColumnFiller(nominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(5))
					.withColumnFiller(polynominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(6))
					.withColumnFiller(binominal, i -> random.nextDouble() > 0.7 ? Double.NaN : random.nextInt(2))
					.build();

			IOTable table = BeltConverter.convert(set, CONTEXT);
			ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(table, true);
			Attribute viewPolynominal = view.getExampleTable().findAttribute("polynominal");
			set.getAttributes().remove(set.getAttributes().get("polynominal"));
			view.getAttributes().remove(view.getAttributes().get("polynominal"));
			set.cleanup();
			view.cleanup();
			set.getExampleTable().removeAttribute(set.getAttributes().get("binominal"));
			view.getExampleTable().removeAttribute(set.getAttributes().get("binominal"));

			RapidAssert.assertEquals(set, view);
			assertEquals(-1, viewPolynominal.getMapping().getIndex(""));
			assertEquals(set.getExampleTable().getAttributeCount(), view.getExampleTable().getAttributeCount());
		}

	}

	public static class Concurrency {

		@Test
		public void testConvertAndAdd() throws InterruptedException {
			ExecutorService executorService = Executors.newFixedThreadPool(4);
			List<AtomicBoolean> result = new ArrayList<>();
			for (int i = 0; i < 1000; i++) {
				CountDownLatch start = new CountDownLatch(4);
				AtomicBoolean failed = new AtomicBoolean(false);
				result.add(failed);

				ExampleSet set = getNumericExampleSet();
				IOTable ioTable = BeltConverter.convert(set, CONTEXT);
				ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = exampleSet.getAttributes().get("integer");
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExample(3).setValue(att, 42);
						assertEquals(42, exampleSet.getExample(3).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att);
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttribute(att);
						exampleSet.getExample(5).setValue(att, 47);
						assertEquals(47, exampleSet.getExample(5).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att);
					Iterator<Example> iterator = exampleSet.iterator();
					iterator.next();
					iterator.next();
					iterator.next();
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttribute(att);
						exampleSet.getExample(3).setValue(att, 41);
						assertEquals(41, iterator.next().getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att);
					Iterator<Example> iterator = exampleSet.iterator();
					iterator.next();
					iterator.next();
					iterator.next();
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttribute(att);
						iterator.next().setValue(att, 411);
						assertEquals(411, exampleSet.getExample(3).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

			}
			executorService.shutdown();
			executorService.awaitTermination(5, TimeUnit.SECONDS);
			for (AtomicBoolean atomicBoolean : result) {
				assertFalse(atomicBoolean.get());
			}
		}

		@Test
		public void testConvertAndAddMultiple() throws InterruptedException {
			ExecutorService executorService = Executors.newFixedThreadPool(3);
			List<AtomicBoolean> result = new ArrayList<>();
			for (int i = 0; i < 1000; i++) {
				CountDownLatch start = new CountDownLatch(2);
				AtomicBoolean failed = new AtomicBoolean(false);
				result.add(failed);

				ExampleSet set = getNumericExampleSet();
				IOTable ioTable = BeltConverter.convert(set, CONTEXT);
				ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = exampleSet.getAttributes().get("integer");
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExample(3).setValue(att, 42);
						assertEquals(42, exampleSet.getExample(3).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att1 = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					Attribute att2 = AttributeFactory.createAttribute("test2", Ontology.NUMERICAL);
					Attribute att3 = AttributeFactory.createAttribute("test3", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att1);
					exampleSet.getAttributes().addRegular(att2);
					exampleSet.getAttributes().addRegular(att3);
					List<Attribute> list = Arrays.asList(att1, att2, att3);
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttributes(list);
						exampleSet.getExample(5).setValue(att2, 47);
						assertEquals(47, exampleSet.getExample(5).getValue(att2), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att1 = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					Attribute att2 = AttributeFactory.createAttribute("test2", Ontology.NUMERICAL);
					Attribute att3 = AttributeFactory.createAttribute("test3", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att1);
					exampleSet.getAttributes().addRegular(att3);
					List<Attribute> list = Arrays.asList(att1, att2, att3);
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttributes(list);
						exampleSet.getExampleTable().removeAttribute(att2);
						exampleSet.getExample(5).setValue(att3, 47);
						assertEquals(47, exampleSet.getExample(5).getValue(att3), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});
			}
			executorService.shutdown();
			executorService.awaitTermination(5, TimeUnit.SECONDS);
			for (AtomicBoolean atomicBoolean : result) {
				assertFalse(atomicBoolean.get());
			}
		}


		@Test
		public void testConvertAndRemove() throws InterruptedException {
			ExecutorService executorService = Executors.newFixedThreadPool(3);
			List<AtomicBoolean> result = new ArrayList<>();
			for (int i = 0; i < 1000; i++) {
				CountDownLatch start = new CountDownLatch(3);
				AtomicBoolean failed = new AtomicBoolean(false);
				result.add(failed);

				ExampleSet set = getNumericExampleSet();
				IOTable ioTable = BeltConverter.convert(set, CONTEXT);
				ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = exampleSet.getAttributes().get("integer");
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExample(3).setValue(att, 42);
						assertEquals(42, exampleSet.getExample(3).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test42", Ontology.NUMERICAL);
					ExampleTable exampleTable = exampleSet.getExampleTable();
					exampleTable.addAttribute(att);
					try {
						assertNotNull(exampleTable.findAttribute(att.getName()));
					} catch (OperatorException e) {
						e.printStackTrace();
					}
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleTable.removeAttribute(att);
						try {
							exampleTable.findAttribute("test42");
							fail();
						} catch (OperatorException e) {
							// good case
						}
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test007", Ontology.NUMERICAL);
					ExampleTable exampleTable = exampleSet.getExampleTable();
					exampleTable.addAttribute(att);
					try {
						assertNotNull(exampleTable.findAttribute(att.getName()));
					} catch (OperatorException e) {
						e.printStackTrace();
					}
					int index = att.getTableIndex();
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleTable.removeAttribute(index);
						try {
							exampleTable.findAttribute("test007");
							fail();
						} catch (OperatorException e) {
							// good case
						}
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

			}
			executorService.shutdown();
			executorService.awaitTermination(5, TimeUnit.SECONDS);
			for (AtomicBoolean atomicBoolean : result) {
				assertFalse(atomicBoolean.get());
			}
		}

		@Test
		public void testEverything() throws InterruptedException {
			ExecutorService executorService = Executors.newFixedThreadPool(9);
			List<AtomicBoolean> result = new ArrayList<>();
			for (int i = 0; i < 1000; i++) {
				CountDownLatch start = new CountDownLatch(8); //intentionally one lower so that one might start later
				AtomicBoolean failed = new AtomicBoolean(false);
				result.add(failed);

				ExampleSet set = getNumericExampleSet();
				IOTable ioTable = BeltConverter.convert(set, CONTEXT);
				ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = exampleSet.getAttributes().get("integer");
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExample(3).setValue(att, 42);
						assertEquals(42, exampleSet.getExample(3).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test42", Ontology.NUMERICAL);
					ExampleTable exampleTable = exampleSet.getExampleTable();
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleTable.addAttribute(att);
						exampleTable.removeAttribute(att);
						try {
							exampleTable.findAttribute("test42");
							fail();
						} catch (OperatorException e) {
							// good case
						}
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test007", Ontology.NUMERICAL);
					ExampleTable exampleTable = exampleSet.getExampleTable();
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
					exampleTable.addAttribute(att);
					int index = att.getTableIndex();
						exampleTable.removeAttribute(index);
						try {
							exampleTable.findAttribute("test007");
							fail();
						} catch (OperatorException e) {
							// good case
						}
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = exampleSet.getAttributes().get("integer");
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExample(3).setValue(att, 42);
						assertEquals(42, exampleSet.getExample(3).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att1 = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					Attribute att2 = AttributeFactory.createAttribute("test2", Ontology.NUMERICAL);
					Attribute att3 = AttributeFactory.createAttribute("test3", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att1);
					exampleSet.getAttributes().addRegular(att2);
					exampleSet.getAttributes().addRegular(att3);
					List<Attribute> list = Arrays.asList(att1, att2, att3);
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttributes(list);
						exampleSet.getExample(5).setValue(att2, 47);
						assertEquals(47, exampleSet.getExample(5).getValue(att2), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att1 = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					Attribute att2 = AttributeFactory.createAttribute("test2", Ontology.NUMERICAL);
					Attribute att3 = AttributeFactory.createAttribute("test3", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att1);
					exampleSet.getAttributes().addRegular(att3);
					List<Attribute> list = Arrays.asList(att1, att2, att3);
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttributes(list);
						exampleSet.getExampleTable().removeAttribute(att2);
						exampleSet.getExample(5).setValue(att3, 47);
						assertEquals(47, exampleSet.getExample(5).getValue(att3), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att);
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttribute(att);
						exampleSet.getExample(5).setValue(att, 47);
						assertEquals(47, exampleSet.getExample(5).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att);
					Iterator<Example> iterator = exampleSet.iterator();
					iterator.next();
					iterator.next();
					iterator.next();
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttribute(att);
						exampleSet.getExample(3).setValue(att, 41);
						assertEquals(41, iterator.next().getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

				executorService.submit(() -> {
					ExampleSet exampleSet = (ExampleSet) view.clone();
					Attribute att = AttributeFactory.createAttribute("test", Ontology.NUMERICAL);
					exampleSet.getAttributes().addRegular(att);
					Iterator<Example> iterator = exampleSet.iterator();
					iterator.next();
					iterator.next();
					iterator.next();
					start.countDown();
					try {
						start.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						exampleSet.getExampleTable().addAttribute(att);
						iterator.next().setValue(att, 411);
						assertEquals(411, exampleSet.getExample(3).getValue(att), 0);
					} catch (Throwable e) {
						e.printStackTrace();
						failed.set(true);
					}
				});

			}
			executorService.shutdown();
			executorService.awaitTermination(5, TimeUnit.SECONDS);
			for (AtomicBoolean atomicBoolean : result) {
				assertFalse(atomicBoolean.get());
			}
		}
	}

	private static ExampleSet getNumericExampleSet() {
		Attribute real = AttributeFactory.createAttribute("real", Ontology.REAL);
		Attribute integer = AttributeFactory.createAttribute("integer", Ontology.INTEGER);
		Attribute dateTime = AttributeFactory.createAttribute("date_time", Ontology.DATE_TIME);
		Attribute date = AttributeFactory.createAttribute("date", Ontology.DATE);
		Attribute time = AttributeFactory.createAttribute("time", Ontology.TIME);
		List<Attribute> attributes = Arrays.asList(real, dateTime, date, time, integer);
		return ExampleSets.from(attributes).withBlankSize(150)
				.withColumnFiller(real, i -> Math.random() > 0.7 ? Double.NaN : 42 + Math.random())
				.withColumnFiller(integer, i -> Math.random() > 0.7 ? Double.NaN : Math.round(Math.random() * 100))
				.withColumnFiller(dateTime,
						i -> Math.random() > 0.7 ? Double.NaN : 1515410698d + Math.floor(Math.random() * 1000))
				.withColumnFiller(date, i -> Math.random() > 0.7 ? Double.NaN :
						230169600000d + Math.floor(Math.random() * 100) * 1000d * 60 * 60 * 24)
				.withColumnFiller(time,
						i -> Math.random() > 0.7 ? Double.NaN : Math.floor(Math.random() * 60 * 60 * 24 * 1000))
				.build();
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
