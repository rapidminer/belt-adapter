/**
 * Copyright (C) 2001-2020 by RapidMiner and the contributors
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

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.AttributeRole;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.SimpleAttributes;
import com.rapidminer.example.set.SimpleExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.DataRowReader;
import com.rapidminer.example.table.ExampleTable;
import com.rapidminer.example.table.internal.CleanableExampleTable;
import com.rapidminer.example.table.internal.ColumnarExampleTable;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.error.AttributeNotFoundError;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.Tools;
import com.rapidminer.tools.att.AttributeSet;


/**
 * {@link ExampleTable} that wraps either a belt {@link Table} plus a {@link ColumnarExampleTable} of additional columns
 * or just a {@link ColumnarExampleTable} containing the converted belt {@link Table} and the additional columns. This
 * allows to read the values of the belt table and add and fill additional columns as for a normal {@link ExampleTable}.
 * The conversion is only done if a {@link DataRow#set(int, double, double)} is called for a belt column. This case is a
 * bug anyway, since one of the unwritten rules of {@link ExampleSet}s is not to write into columns that have not been
 * added.
 *
 * @author Gisa Meier
 * @since 0.7
 */
class ConvertOnWriteExampleTable implements CleanableExampleTable {

	/**
	 * the accessor for the belt table
	 */
	private transient volatile AbstractTableAccessor tableAccessor;

	/**
	 * Extra table for adding new columns. We cannot add those to the belt table since in the belt API they are
	 * immutable and in the example table API they are added first and then filled
	 */
	private volatile ColumnarExampleTable newColumns;

	/**
	 * the whole converted table (converted belt table plus newColumns)
	 */
	private volatile ColumnarExampleTable convertedTable;

	/**
	 * the width of the underlying belt table
	 */
	private final int originalWidth;

	/**
	 * the table height, could be reconstructed from the convertedTable or the tableAccessor but stored for performance
	 * reasons
	 */
	private final int height;


	/**
	 * Locks to prevent a convert in parallel to a write of the new columns table
	 */
	private final transient ReadWriteLock rwLock = new ReentrantReadWriteLock();
	/**
	 * Used for writing to the new Columns table, arbitrary many in parallel but not at the same time as convert
	 */
	private final transient Lock readLock = rwLock.readLock();
	/**
	 * Used for convert(), only one thread at a time
	 */
	private final transient Lock writeLock = rwLock.writeLock();

	/**
	 * Lock object to prevent several newColumns tables being constructed in parallel. There can only be one.
	 */
	private final transient Object newColumnsTableLock = new Object();

	/**
	 * Creates a new convert on write table based on the given belt table. The attributes in the list are neither cloned
	 * nor is their table index changed.
	 *
	 * @param table
	 * 		the belt table to wrap
	 * @param attributeList
	 * 		the list of attributes fitting to the belt table
	 * @param numberOfDatetime
	 * 		the number of date-time columns
	 */
	ConvertOnWriteExampleTable(Table table, List<Attribute> attributeList, int numberOfDatetime) {
		if (numberOfDatetime > 0) {
			tableAccessor = new MixedTableAccessor(table, attributeList, numberOfDatetime, 0);
		} else {
			tableAccessor = new NumericTableAccessor(table, attributeList, 0);
		}
		originalWidth = table.width();
		height = table.height();
	}

	/**
	 * Copy-constructor.
	 */
	private ConvertOnWriteExampleTable(AbstractTableAccessor tableAccessor, ColumnarExampleTable convertedTable,
									   ColumnarExampleTable newColumns, int originalWidth, int originalHeight) {
		this.tableAccessor = tableAccessor;
		this.convertedTable = convertedTable;
		this.newColumns = newColumns;
		this.originalWidth = originalWidth;
		this.height = originalHeight;
	}

	@Override
	public int size() {
		return height;
	}

	@Override
	public DataRowReader getDataRowReader() {
		if (convertedTable != null) {
			return convertedTable.getDataRowReader();
		}
		return new DataRowReader() {

			//holds the reference as long as the reader is alive
			private Object beltReader = getReader(tableAccessor);

			private int index = 0;

			@Override
			public boolean hasNext() {
				return index < height;
			}

			@Override
			public DataRow next() {
				final int currentRow = index++;
				if (convertedTable != null) {
					return convertedTable.getDataRow(currentRow);
				}
				return new DataRow() {

					@Override
					protected double get(int columnIndex, double defaultValue) {
						return getValue(columnIndex, currentRow, beltReader);
					}

					@Override
					protected void set(int columnIndex, double value, double defaultValue) {
						readLock.lock();
						try {
							if (newColumns != null && columnIndex >= originalWidth) {
								newColumns.getDataRow(currentRow).set(newColumns.getAttribute(columnIndex - originalWidth)
										, value);
								return;
							}
						} finally {
							readLock.unlock();
						}
						if (convertedTable == null) {
							convert();
							// delete the reader reference
							beltReader = null;
						}
						convertedTable.getDataRow(currentRow).set(convertedTable.getAttribute(columnIndex), value);
					}

					@Override
					protected void ensureNumberOfColumns(int i) {
						//not necessary, converted table and newColumns are {@link ColumnarExampleTable}
					}

					@Override
					public int getType() {
						return DataRowFactory.TYPE_COLUMN_VIEW;
					}

					@Override
					public String toString() {
						StringJoiner result = new StringJoiner(",");
						for (int i = 0; i < getNumberOfAttributes(); i++) {
							result.add("" + get(i, 0));
						}
						return result.toString();
					}
				};
			}

		};

	}

	@Override
	public DataRow getDataRow(int rowIndex) {
		if (convertedTable != null) {
			return convertedTable.getDataRow(rowIndex);
		}
		return new DataRow() {

			// holds the reader reference as long as the row is alive
			private transient Object reader = getUnbufferedReader(tableAccessor);

			@Override
			protected double get(int columnIndex, double defaultValue) {
				return getValue(columnIndex, rowIndex, reader);
			}

			@Override
			protected void set(int columnIndex, double value, double defaultValue) {
				readLock.lock();
				try {
					if (newColumns != null && columnIndex >= originalWidth) {
						newColumns.getDataRow(rowIndex).set(newColumns.getAttribute(columnIndex - originalWidth), value);
						return;
					}
				} finally {
					readLock.unlock();
				}

				if (convertedTable == null) {
					convert();
					// delete the reader reference
					reader = null;
				}
				convertedTable.getDataRow(rowIndex).set(convertedTable.getAttribute(columnIndex), value);
			}

			@Override
			protected void ensureNumberOfColumns(int i) {
				//not necessary, converted table and newColumns are {@link ColumnarExampleTable}
			}

			@Override
			public int getType() {
				return DataRowFactory.TYPE_COLUMN_VIEW;
			}

			@Override
			public String toString() {
				StringJoiner result = new StringJoiner(",");
				for (int i = 0; i < getNumberOfAttributes(); i++) {
					result.add("" + get(i, 0));
				}
				return result.toString();
			}
		};
	}

	@Override
	public void addAttributes(Collection<Attribute> collection) {
		readLock.lock();
		try {
			if (convertedTable != null) {
				convertedTable.addAttributes(collection);
				return;
			}
			if (newColumns == null) {
				createNewColumns();
			}
			newColumns.addAttributes(collection);
			for (Attribute attribute : collection) {
				int internalIndex = attribute.getTableIndex();
				attribute.setTableIndex(originalWidth + internalIndex);
			}
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public int addAttribute(Attribute attribute) {
		readLock.lock();
		try {
			if (convertedTable != null) {
				return convertedTable.addAttribute(attribute);
			}
			if (newColumns == null) {
				createNewColumns();
			}
			newColumns.addAttribute(attribute);
			int tableIndex = attribute.getTableIndex();
			int shiftedIndex = tableIndex + originalWidth;
			attribute.setTableIndex(shiftedIndex);
			return shiftedIndex;
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public void removeAttribute(Attribute attribute) {
		if (convertedTable != null) {
			convertedTable.removeAttribute(attribute);
			return;
		}
		readLock.lock();
		try {
			if (attribute.getTableIndex() >= originalWidth && newColumns != null) {
				newColumns.removeAttribute(attribute.getTableIndex() - originalWidth);
				return;
			}
		} finally {
			readLock.unlock();
		}

		if (convertedTable == null) {
			convert();
		}
		convertedTable.removeAttribute(attribute);

	}

	@Override
	public void removeAttribute(int i) {
		if (convertedTable != null) {
			convertedTable.removeAttribute(i);
			return;
		}
		readLock.lock();
		try {
			if (i >= originalWidth && newColumns != null) {
				newColumns.removeAttribute(i - originalWidth);
				return;
			}
		} finally {
			readLock.unlock();
		}

		if (convertedTable == null) {
			convert();
		}
		convertedTable.removeAttribute(i);
	}

	@Override
	public Attribute[] getAttributes() {
		// store references so that they do not change in parallel
		ColumnarExampleTable newColumnsRef = this.newColumns;
		AbstractTableAccessor tableAccessorRef = this.tableAccessor;
		ColumnarExampleTable convertedTableRef = this.convertedTable;
		if (convertedTableRef != null) {
			return convertedTableRef.getAttributes();
		}
		if (newColumnsRef == null) {
			return tableAccessorRef.getAttributes();
		}
		Attribute[] newAttributes = newColumnsRef.getAttributes();
		Attribute[] tableAttributes = tableAccessorRef.getAttributes();
		Attribute[] attributes = Arrays.copyOf(tableAttributes, tableAttributes.length + newAttributes.length);
		for (int i = 0; i < newAttributes.length; i++) {
			Attribute newAttribute = newAttributes[i];
			if (newAttribute != null) {
				newAttribute = (Attribute) newAttribute.clone();
				newAttribute.setTableIndex(originalWidth + newAttribute.getTableIndex());
			}
			attributes[i + originalWidth] = newAttribute;
		}
		return attributes;
	}

	@Override
	public Attribute getAttribute(int i) {
		// store references so that they do not change in parallel
		ColumnarExampleTable newColumnsRef = this.newColumns;
		AbstractTableAccessor tableAccessorRef = this.tableAccessor;
		ColumnarExampleTable convertedTableRef = this.convertedTable;
		if (convertedTableRef != null) {
			return convertedTableRef.getAttribute(i);
		}
		if (newColumnsRef == null || i < originalWidth) {
			return tableAccessorRef.getAttribute(i);
		}
		Attribute attribute = newColumnsRef.getAttribute(i - originalWidth);
		attribute = (Attribute) attribute.clone();
		attribute.setTableIndex(attribute.getTableIndex() + originalWidth);
		return attribute;
	}

	@Override
	public Attribute findAttribute(String s) throws OperatorException {
		// store references so that they do not change in parallel
		ColumnarExampleTable newColumnsRef = this.newColumns;
		AbstractTableAccessor tableAccessorRef = this.tableAccessor;
		ColumnarExampleTable convertedTableRef = this.convertedTable;
		if (convertedTableRef != null) {
			return convertedTableRef.findAttribute(s);
		}
		if (newColumnsRef == null) {
			return tableAccessorRef.findAttribute(s);
		}
		try {
			return tableAccessorRef.findAttribute(s);
		} catch (AttributeNotFoundError e) {
			Attribute attribute = newColumnsRef.findAttribute(s);
			attribute = (Attribute) attribute.clone();
			attribute.setTableIndex(attribute.getTableIndex() + originalWidth);
			return attribute;
		}
	}

	@Override
	public int getNumberOfAttributes() {
		// store references so that they do not change in parallel
		ColumnarExampleTable newColumnsRef = this.newColumns;
		ColumnarExampleTable convertedTableRef = this.convertedTable;
		if (convertedTableRef != null) {
			return convertedTableRef.getNumberOfAttributes();
		}
		if (newColumnsRef == null) {
			return originalWidth;
		}
		return originalWidth + newColumnsRef.getNumberOfAttributes();
	}

	@Override
	public int getAttributeCount() {
		// store references so that they do not change in parallel
		AbstractTableAccessor tableAccessorRef = this.tableAccessor;
		ColumnarExampleTable newColumnsRef = this.newColumns;
		ColumnarExampleTable convertedTableRef = this.convertedTable;
		if (convertedTableRef != null) {
			return convertedTableRef.getAttributeCount();
		}
		if (newColumnsRef == null) {
			return originalWidth - tableAccessorRef.getUnused();
		}
		return originalWidth - tableAccessorRef.getUnused() + newColumnsRef.getAttributeCount();
	}

	// the following 6 methods are the same as in {@link AbstractExampleTable}
	@Override
	public ExampleSet createExampleSet(Attribute labelAttribute) {
		return createExampleSet(labelAttribute, null, null);
	}

	@Override
	public ExampleSet createExampleSet(Iterator<AttributeRole> newSpecialAttributes) {
		Map<Attribute, String> specialAttributes = new LinkedHashMap<>();
		while (newSpecialAttributes.hasNext()) {
			AttributeRole role = newSpecialAttributes.next();
			specialAttributes.put(role.getAttribute(), role.getSpecialName());
		}
		return new SimpleExampleSet(this, specialAttributes);
	}

	@Override
	public ExampleSet createExampleSet(Attribute labelAttribute, Attribute weightAttribute, Attribute idAttribute) {
		Map<Attribute, String> specialAttributes = new LinkedHashMap<>();
		if (labelAttribute != null) {
			specialAttributes.put(labelAttribute, Attributes.LABEL_NAME);
		}
		if (weightAttribute != null) {
			specialAttributes.put(weightAttribute, Attributes.WEIGHT_NAME);
		}
		if (idAttribute != null) {
			specialAttributes.put(idAttribute, Attributes.ID_NAME);
		}
		return new SimpleExampleSet(this, specialAttributes);
	}

	@Override
	public ExampleSet createExampleSet(AttributeSet attributeSet) {
		Map<Attribute, String> specialAttributes = new LinkedHashMap<>();
		Iterator<String> i = attributeSet.getSpecialNames().iterator();
		while (i.hasNext()) {
			String name = i.next();
			specialAttributes.put(attributeSet.getSpecialAttribute(name), name);
		}
		return createExampleSet(specialAttributes);
	}

	@Override
	public ExampleSet createExampleSet(Map<Attribute, String> map) {
		return new SimpleExampleSet(this, map);
	}

	@Override
	public ExampleSet createExampleSet() {
		return createExampleSet(Collections.<Attribute, String>emptyMap());
	}

	@Override
	public String toString() {
		return "ExampleTable, " + getNumberOfAttributes() + " attributes, " + size() + " data rows," + Tools.getLineSeparator()
				+ "attributes: " + Arrays.toString(getAttributes());
	}

	@Override
	public String toDataString() {
		StringBuilder result = new StringBuilder(toString() + Tools.getLineSeparator());
		DataRowReader reader = getDataRowReader();
		while (reader.hasNext()) {
			result.append(reader.next().toString()).append(Tools.getLineSeparator());
		}
		return result.toString();
	}

	@Override
	public ExampleTable columnCleanupClone(Attributes attributes) {
		return cleanup(newColumns, tableAccessor, convertedTable, attributes);
	}

	/**
	 * @return the underlying belt table if it still exists
	 */
	Table getTable() {
		AbstractTableAccessor accessor = tableAccessor;
		if (accessor != null) {
			return accessor.getTable();
		}
		return null;
	}

	/**
	 * @return the example table of additional columns
	 */
	ColumnarExampleTable getNewColumns() {
		return newColumns;
	}

	/**
	 * Before we can serialize, we need to get rid of the {@link #tableAccessor} since it contains a belt {@link Table}
	 * which cannot be written with standard java serialization.
	 */
	private void writeObject(ObjectOutputStream oos) throws IOException {
		if (tableAccessor != null) {
			convert();
		}
		oos.defaultWriteObject(); // Calling the default serialization logic

	}

	/**
	 * Creates the new columns table
	 */
	private void createNewColumns() {
		synchronized (newColumnsTableLock) {
			if (newColumns == null) {
				ColumnarExampleTable newColumnsRef = new ColumnarExampleTable(new ArrayList<>());
				newColumnsRef.setExpectedSize(size());
				newColumnsRef.addBlankRows(size());
				this.newColumns = newColumnsRef;
			}
		}
	}

	/**
	 * Converts to one columnar example table containing the belt table values and the new columns.
	 */
	private void convert() {
		writeLock.lock();
		try {
			if (convertedTable == null) {
				ColumnarExampleTable newConvertedTable = FromTableConverter.convert(tableAccessor.getTable(), tableAccessor.getAttributes());
				ColumnarExampleTable newColumnsRef = newColumns;
				if (newColumnsRef != null) {
					List<Attribute> dummyAttributes = new ArrayList<>();
					// add dummy attributes to prevent adding into the holes of the table accessor attributes,
					// addAttribute fills holes first before adding at the end
					for (int i = 0; i < tableAccessor.getUnused(); i++) {
						Attribute dummy = AttributeFactory.createAttribute("", Ontology.NUMERICAL);
						newConvertedTable.addAttribute(dummy);
						dummyAttributes.add(dummy);
					}
					for (Attribute attribute : newColumnsRef.getAttributes()) {
						if (attribute != null) {
							Attribute clone = (Attribute) attribute.clone();
							newConvertedTable.addAttribute(clone);
							newConvertedTable.fillColumn(clone, j -> newColumnsRef.getDataRow(j).get(attribute));
						} else {
							//add dummy attribute to keep table indices
							Attribute dummy = AttributeFactory.createAttribute("", Ontology.NUMERICAL);
							newConvertedTable.addAttribute(dummy);
							dummyAttributes.add(dummy);
						}
					}
					if (!dummyAttributes.isEmpty()) {
						for (Attribute dummy : dummyAttributes) {
							newConvertedTable.removeAttribute(dummy);
						}
					}

				}
				convertedTable = newConvertedTable;
				tableAccessor = null;
				newColumns = null;
			}
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * Gets a value either from the convertedTable, or from the belt table using the beltReader or from the newColumns.
	 */
	private double getValue(int columnIndex, int currentRow, Object beltReader) {
		// store references so that they do not change in parallel
		ColumnarExampleTable newColumnsRef = this.newColumns;
		AbstractTableAccessor tableAccessorRef = this.tableAccessor;
		ColumnarExampleTable convertedTableRef = this.convertedTable;
		if (convertedTableRef != null) {
			return convertedTableRef.getDataRow(currentRow).get(convertedTableRef.getAttribute(columnIndex));
		}
		if (columnIndex < originalWidth) {
			return tableAccessorRef.get(currentRow, columnIndex, beltReader);
		}
		return newColumnsRef.getDataRow(currentRow).get(newColumnsRef.getAttribute(columnIndex - originalWidth));
	}

	/**
	 * Gets a reader or {@code null} if {@link #convert()} has already been called.
	 */
	private static Object getReader(AbstractTableAccessor tableAccessor) {
		if (tableAccessor != null) {
			return tableAccessor.getReaders();
		}
		return null;
	}

	/**
	 * Gets an unbuffered reader or {@code null} if {@link #convert()} has already been called.
	 */
	private static Object getUnbufferedReader(AbstractTableAccessor wrapperTable) {
		if (wrapperTable != null) {
			return wrapperTable.getUnbufferedReaders();
		}
		return null;
	}

	/**
	 * Cleans up the columns. Has the volatile variables as parameter so that they do not change in between.
	 */
	private ExampleTable cleanup(ColumnarExampleTable newColumns, AbstractTableAccessor tableWrapper, ColumnarExampleTable convertedTable,
								 Attributes attributes) {
		if (convertedTable != null) {
			ColumnarExampleTable newConvertedTable = convertedTable.columnCleanupClone(attributes);
			return new ConvertOnWriteExampleTable(null, newConvertedTable, null, originalWidth, height);
		}

		AbstractTableAccessor newTableWrapper = tableWrapper.columnCleanupClone(attributes);
		ColumnarExampleTable newNewColumns = null;
		if (newColumns != null) {
			//clean up new columns table, requires shifting of attributes
			Attributes newColumnsAttributes = new SimpleAttributes();
			for (Iterator<Attribute> allIterator = attributes.allAttributes(); allIterator.hasNext(); ) {
				Attribute attribute = allIterator.next();
				if (attribute.getTableIndex() >= originalWidth) {
					Attribute shiftedAttribute = (Attribute) attribute.clone();
					shiftedAttribute.setTableIndex(shiftedAttribute.getTableIndex() - originalWidth);
					newColumnsAttributes.addRegular(shiftedAttribute);
				}
			}
			newNewColumns = newColumns.columnCleanupClone(newColumnsAttributes);
		}
		return new ConvertOnWriteExampleTable(newTableWrapper, null, newNewColumns, originalWidth, height);
	}
}