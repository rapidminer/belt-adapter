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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;

import org.apache.commons.math3.util.MathArrays;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.rapidminer.adaption.belt.ContextAdapter;
import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.util.ColumnAnnotation;
import com.rapidminer.belt.util.ColumnRole;
import com.rapidminer.belt.util.Order;
import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.core.concurrency.ExecutionStoppedException;
import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.set.MappedExampleSet;
import com.rapidminer.example.set.Partition;
import com.rapidminer.example.set.SortedExampleSet;
import com.rapidminer.example.set.SplittedExampleSet;
import com.rapidminer.example.table.AbstractAttribute;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.NominalMapping;
import com.rapidminer.example.table.ViewAttribute;
import com.rapidminer.operator.ViewModel;
import com.rapidminer.operator.preprocessing.filter.NominalToNumeric;
import com.rapidminer.operator.preprocessing.filter.NominalToNumericModel;
import com.rapidminer.test.asserter.AsserterFactoryRapidMiner;
import com.rapidminer.test_utils.RapidAssert;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.ParameterService;


/**
 * Tests the {@link BeltConverter#convert(ExampleSet, ConcurrencyContext)} method when it is applied after
 * {@link TableViewCreator#convertOnWriteView(IOTable, boolean)}.
 *
 * @author Gisa Meier
 */
public class ViewToTableConverterTest {

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
		ParameterService.init();
	}

	@Test
	public void testSimple() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

		IOTable converted = BeltConverter.convert(view, CONTEXT);

		RapidAssert.assertEquals(ioTable, converted);
	}

	@Test
	public void testRemove() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
		view.getAttributes().remove(view.getAttributes().get("integer"));

		IOTable converted = BeltConverter.convert(view, CONTEXT);

		Table expected = Builders.newTableBuilder(table).remove("integer").build(ContextAdapter.adapt(CONTEXT));
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testRemoveFromTable() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
		Attribute textset = view.getAttributes().get("textSet");
		view.getAttributes().remove(textset);
		view.getExampleTable().removeAttribute(textset);
		view.getAttributes().remove(view.getAttributes().get("time"));

		IOTable converted = BeltConverter.convert(view, CONTEXT);
		Context ctx = ContextAdapter.adapt(CONTEXT);
		Table expected = Builders.newTableBuilder(table).remove("textSet").remove("time")
				.replace("date-time", table.transform("date-time").applyObjectToDateTime(Instant.class,
						i -> i == null ? null : Instant.ofEpochMilli(i.toEpochMilli()), ctx).toColumn()).build(ctx);
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testAddToFromTable() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
		Attribute attribute = AttributeFactory.createAttribute("new", Ontology.NUMERICAL);
		view.getAttributes().addRegular(attribute);
		view.getExampleTable().addAttribute(attribute);
		int index = 0;
		for (Example example : view) {
			example.setValue(attribute, 0.231 * index++);
		}

		IOTable converted = BeltConverter.convert(view, CONTEXT);
		Table expected = Builders.newTableBuilder(table).addReal("new", i -> 0.231 * i).build(ContextAdapter.adapt(CONTEXT));
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testReorder() {
		Table table = getTable();
		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
			Attribute attribute = AttributeFactory.createAttribute("new", Ontology.NUMERICAL);
			view.getAttributes().addRegular(attribute);
			view.getExampleTable().addAttribute(attribute);
			int index =0;
			for (Example example : view) {
				example.setValue(attribute, 0.231*index++);
			}

		ExampleSet clone = (ExampleSet) view.clone();
		Attributes viewAttributes = view.getAttributes();
		List<String> names = new ArrayList<>();
		for (Iterator<Attribute> attributeIterator = viewAttributes.allAttributes(); attributeIterator.hasNext(); ) {
			Attribute next = attributeIterator.next();
			names.add(next.getName());
		}

		Collections.shuffle(names);

		Attributes cloneAttributes = clone.getAttributes();
		cloneAttributes.clearRegular();
		cloneAttributes.clearSpecial();
		for (String name : names) {
			cloneAttributes.add(viewAttributes.getRole(name));
		}

		IOTable converted = BeltConverter.convert(clone, CONTEXT);
		Table expected = Builders.newTableBuilder(table).addReal("new", i -> 0.231 * i).build(ContextAdapter.adapt(CONTEXT));
		expected = expected.columns(names);
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testRename() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
		Attribute integer = view.getAttributes().get("integer");
		//this rename does not work
		//view.getAttributes().rename(integer, "integer2");
		//this is what the Rename operator does
		integer.setName("integer2");
		IOTable converted = BeltConverter.convert(view, CONTEXT);

		Table expected = Builders.newTableBuilder(table).rename("integer", "integer2").build(ContextAdapter.adapt(CONTEXT));
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testChangeRoles() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
		Attribute integer = view.getAttributes().get("integer");
		Attribute textset = view.getAttributes().get("textSet");
		Attribute bool = view.getAttributes().get("boolean");
		// this is what Set Role does
		view.getAttributes().remove(integer);
		view.getAttributes().setSpecialAttribute(integer, Attributes.ID_NAME);
		view.getAttributes().remove(textset);
		view.getAttributes().setSpecialAttribute(textset, "i am special");
		view.getAttributes().remove(bool);
		view.getAttributes().addRegular(bool);
		IOTable converted = BeltConverter.convert(view, CONTEXT);

		Table expected = Builders.newTableBuilder(table).remove("integer")
				.add("integer", table.column("integer"))
				.addMetaData("integer", ColumnRole.ID)
				.remove("textSet").add("textSet", table.column("textSet"))
				.addMetaData("textSet", ColumnRole.METADATA)
				.addMetaData("textSet", new LegacyRole("i am special"))
				.remove("boolean").add("boolean", table.column("boolean"))
				.build(ContextAdapter.adapt(CONTEXT));
		RapidAssert.assertEquals(new IOTable(expected), converted);
		Assert.assertEquals(expected.getMetaData("textSet"), converted.getTable().getMetaData("textSet"));
	}

	@Test
	public void testOneMapped() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

		int[] mapping = new int[table.height()];
		Arrays.setAll(mapping, i -> i);
		MathArrays.shuffle(mapping);

		ExampleSet mapped = new MappedExampleSet(view, mapping);

		IOTable converted = BeltConverter.convert(mapped, CONTEXT);

		Table expected = table.rows(mapping, ContextAdapter.adapt(CONTEXT));
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testTwoMapped() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

		SortedExampleSet sorted = new SortedExampleSet(view, view.getAttributes().get("real"), SortedExampleSet.INCREASING);

		int[] mapping = new int[table.height() / 2];
		Arrays.setAll(mapping, i -> i);
		MathArrays.shuffle(mapping);

		ExampleSet mapped = new MappedExampleSet(sorted, mapping);

		IOTable converted = BeltConverter.convert(mapped, CONTEXT);

		Table expected = table.sort("real", Order.ASCENDING, ContextAdapter.adapt(CONTEXT))
				.rows(mapping, ContextAdapter.adapt(CONTEXT));
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testThreeMapped() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

		SortedExampleSet sorted = new SortedExampleSet(view, view.getAttributes().get("real"), SortedExampleSet.INCREASING);

		int[] mapping = new int[table.height() / 2];
		Arrays.setAll(mapping, i -> i);
		MathArrays.shuffle(mapping);

		ExampleSet mapped = new MappedExampleSet(sorted, mapping);

		int[] elements = new int[mapped.size()];
		Arrays.setAll(elements, i -> i % 3);
		SplittedExampleSet splitted = new SplittedExampleSet(mapped, new Partition(elements, 3));
		splitted.selectSingleSubset(0);

		IOTable converted = BeltConverter.convert(splitted, CONTEXT);

		Table expected = table.sort("real", Order.ASCENDING, ContextAdapter.adapt(CONTEXT))
				.rows(mapping, ContextAdapter.adapt(CONTEXT));
		int[] rows2 = new int[expected.height() / 3 + (expected.height() % 3 > 0 ? 1 : 0)];
		Arrays.setAll(rows2, i -> 3 * i);
		expected = expected.rows(rows2, ContextAdapter.adapt(CONTEXT));
		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testModelViewColumn() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
		ViewModel model = new NominalToNumericModel(view, NominalToNumeric.INTEGERS_CODING);

		Attribute nominal = view.getAttributes().get("nominal");
		view.getAttributes().remove(nominal);
		view.getAttributes().addRegular(new ViewAttribute(model, nominal, nominal.getName(), Ontology.INTEGER, null));

		IOTable converted = BeltConverter.convert(view, CONTEXT);

		Table expected = Builders.newTableBuilder(table).remove("nominal")
				.add("nominal", table.transform("nominal")
						.applyCategoricalToInteger53Bit(i -> i - 1, ContextAdapter.adapt(CONTEXT)).toColumn())
				.build(ContextAdapter.adapt(CONTEXT));

		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testModelViewColumnOnMapped() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);
		ViewModel model = new NominalToNumericModel(view, NominalToNumeric.INTEGERS_CODING);

		int[] mapping = new int[table.height() / 2];
		Arrays.setAll(mapping, i -> 2 * i);

		ExampleSet mapped = new MappedExampleSet(view, mapping);
		Attribute nominal = mapped.getAttributes().get("nominal");
		mapped.getAttributes().remove(nominal);
		mapped.getAttributes().addRegular(new ViewAttribute(model, nominal, nominal.getName(), Ontology.INTEGER, null));

		IOTable converted = BeltConverter.convert(mapped, CONTEXT);

		Context adapted = ContextAdapter.adapt(CONTEXT);
		Table mappedTable = table.rows(mapping, adapted);
		Table expected = Builders.newTableBuilder(mappedTable).remove("nominal")
				.add("nominal", mappedTable.transform("nominal")
						.applyCategoricalToInteger53Bit(i -> i - 1, adapted).toColumn())
				.build(adapted);

		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testIntegerToReal() {
		Table table = getTable();

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

		Attribute attribute = view.getAttributes().get("integer");

		//That is what Numeric2Real does
		Attribute newAttribute = AttributeFactory.changeValueType(attribute, Ontology.REAL);
		view.getAttributes().replace(attribute, newAttribute);

		IOTable converted = BeltConverter.convert(view, CONTEXT);

		Table expected = Builders.newTableBuilder(table).replace("integer", Buffers.realBuffer(table.column("integer")).toColumn())
				.build(ContextAdapter.adapt(CONTEXT));

		RapidAssert.assertEquals(new IOTable(expected), converted);
	}

	@Test
	public void testNotSafeColumn() {
		Table table = getTable();
		Context ctx = ContextAdapter.adapt(CONTEXT);
		table = Builders.newTableBuilder(table).remove("textSet").remove("time")
				.add("numeric", table.column("real"))
				.addMetaData("numeric", LegacyType.NUMERICAL)
				.add("nominal2", table.column("nominal"))
				.addMetaData("nominal2", LegacyType.NOMINAL)
				.add("studio date", table.column("date-time"))
				.addMetaData("studio date", LegacyType.DATE)
				.add("studio time", table.column("date-time"))
				.addMetaData("studio time", LegacyType.TIME)
				.removeMetaData("boolean", ColumnRole.class)
				.addMetaData("boolean", LegacyType.BINOMINAL)
				.addMetaData("boolean", ColumnRole.LABEL)
				.build(ctx);

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

		Attributes viewAttributes = view.getAttributes();
		List<String> names = new ArrayList<>();
		for (Iterator<Attribute> attributeIterator = viewAttributes.allAttributes(); attributeIterator.hasNext(); ) {
			Attribute next = attributeIterator.next();
			names.add(next.getName());
		}

		for (String name : names) {
			Attribute attribute = viewAttributes.get(name);
			AttributeRole role = viewAttributes.getRole(attribute);
			DummyAttribute newAttribute = new DummyAttribute(attribute);
			viewAttributes.remove(attribute);
			if (role.isSpecial()) {
				viewAttributes.setSpecialAttribute(newAttribute, role.getSpecialName());
			} else {
				viewAttributes.addRegular(newAttribute);
			}
		}

		IOTable converted = BeltConverter.convert(view, CONTEXT);

		//loosing nanoseconds when reading via studio wrapper
		DateTimeColumn studioDateTimeColumn = table.transform("date-time")
				.applyObjectToDateTime(Instant.class, i -> i == null ? null : Instant.ofEpochMilli(i.toEpochMilli()), ctx).toColumn();
		DateTimeColumn studioDateColumn = table.transform("date-time")
				.applyObjectToDateTime(Instant.class, i -> i == null ? null : Instant.ofEpochSecond(i.getEpochSecond()), ctx).toColumn();
		Table expected = Builders.newTableBuilder(table).replace("date-time", studioDateTimeColumn)
				.replace("studio date", studioDateColumn)
				.replace("studio time", studioDateTimeColumn).build(ctx);

		RapidAssert.assertEquals(new IOTable(expected), converted);
		Assert.assertEquals(expected.getMetaData(), converted.getTable().getMetaData());
	}

	@Test
	public void testEmpty() {
		Table table = new Table(1234);

		IOTable ioTable = new IOTable(table);
		ExampleSet view = TableViewCreator.INSTANCE.convertOnWriteView(ioTable, false);

		IOTable converted = BeltConverter.convert(view, CONTEXT);

		RapidAssert.assertEquals(new IOTable(table), converted);
	}


	private static class DummyAttribute extends AbstractAttribute {

		private final Attribute attribute;

		private DummyAttribute(Attribute attribute) {
			super(attribute.getName(), attribute.getValueType());
			this.attribute = attribute;
		}

		@Override
		public Object clone() {
			return attribute.clone();
		}

		@Override
		public int getTableIndex() {
			return attribute.getTableIndex();
		}

		@Override
		public NominalMapping getMapping() {
			return attribute.getMapping();
		}

		@Override
		public void setMapping(NominalMapping nominalMapping) {
			attribute.setMapping(nominalMapping);
		}

		@Override
		public boolean isNominal() {
			return attribute.isNominal();
		}

		@Override
		public boolean isNumerical() {
			return attribute.isNumerical();
		}

		@Override
		public boolean isDateTime() {
			return attribute.isDateTime();
		}

		@Override
		public String getAsString(double value, int digits, boolean quoteNominal) {
			return attribute.getAsString(value, digits, quoteNominal);
		}
	}


	private static Table getTable() {
		TableBuilder builder = Builders.newTableBuilder(111);
		builder.addNominal("nominal", i -> "value" + (i % 10));
		builder.addBoolean("boolean", i -> "val" + (i % 2), "val1");
		builder.addReal("real", i -> Math.random() > 0.7 ? Double.NaN : Math.random());
		builder.addInt53Bit("integer", i -> Math.random() > 0.7 ? Double.NaN : Math.random() * 1000);
		builder.addTime("time",
				i -> Math.random() > 0.7 ? null : LocalTime.of((int) (Math.random() * 24), (int) (Math.random() * 60), (int) (Math.random() * 60), (int) (Math.random() * 100000)));
		builder.addDateTime("date-time", i -> Math.random() > 0.7 ? null : Instant.ofEpochSecond((long) (Math.random() * 1587727537), (long) (Math.random() * 999999999)));
		builder.addTextset("textSet", i -> new StringSet(Arrays.asList("val" + i, "value" + i, "val" + (i - 1))));
		builder.addMetaData("boolean", ColumnRole.LABEL);
		builder.addMetaData("real", ColumnRole.LABEL);
		builder.addMetaData("time", new ColumnAnnotation("blablup"));
		builder.addMetaData("real", new ColumnAnnotation("blablup"));
		return builder.build(ContextAdapter.adapt(CONTEXT));
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
