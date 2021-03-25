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

import java.time.Instant;
import java.util.Calendar;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rapidminer.RapidMiner;
import com.rapidminer.adaption.belt.IOTable;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.util.ColumnReference;
import com.rapidminer.belt.util.ColumnRole;
import com.rapidminer.core.concurrency.ConcurrencyContext;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.set.HeaderExampleSet;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.ParameterService;
import com.rapidminer.tools.Tools;
import com.rapidminer.tools.parameter.ParameterChangeListener;


/**
 * Converts between {@link ExampleSet}s and belt {@link Table}s.
 *
 * Please note that this class is not part of any public API and might be modified or removed in future releases without
 * prior warning.
 *
 * @author Gisa Meier
 */
public final class BeltConverter {

	/**
	 * The standard belt types.
	 */
	public static final Set<Column.TypeId> STANDARD_TYPES = EnumSet.of(Column.TypeId.REAL,
			Column.TypeId.INTEGER_53_BIT,
			Column.TypeId.NOMINAL, Column.TypeId.DATE_TIME, Column.TypeId.TIME);

	/**
	 * Marker exception for conversion failure from belt {@link Table} to {@link ExampleSet}. Happens only if the belt
	 * table contains advanced columns.
	 */
	public static class ConversionException extends UnsupportedOperationException {

		private final String columnName;
		private final transient ColumnType<?> type;

		ConversionException(String columnName, ColumnType<?> type) {
			super("Failed to convert Table because of advanced column '" + columnName + "' of " + type);
			this.columnName = columnName;
			this.type = type;
		}

		/**
		 * @return the name of the column that failed to convert
		 */
		public String getColumnName() {
			return columnName;
		}

		/**
		 * @return the type of the column that failed to convert
		 */
		public ColumnType<?> getType() {
			return type;
		}
	}

	/**
	 * key for storing belt column meta data in the user data of an {@link ExampleSet}
	 */
	static final String IOOBJECT_USER_DATA_COLUMN_META_DATA_KEY = BeltConverter.class.getName() + ".column_meta_data";

	/**
	 * Prefix of the role names of confidence attributes
	 */
	static final String CONFIDENCE_PREFIX = Attributes.CONFIDENCE_NAME + "_";

	/**
	 * Pattern used to check if a studio role contains an index that needs to be removed before converting to belt.
	 */
	private static final Pattern INDEX_PATTERN = Pattern.compile("(.+)_[0-9]+");

	/**
	 * String into which {@link ColumnRole#METADATA} is converted
	 */
	private static final String META_DATA_NAME = "metadata";

	/**
	 * String into which {@link ColumnRole#INTERPRETATION} is converted
	 */
	static final String INTERPRETATION_NAME = "interpretation";

	/**
	 * String into which {@link ColumnRole#ENCODING} is converted
	 */
	static final String ENCODING_NAME = "encoding";

	/**
	 * String into which {@link ColumnRole#SOURCE} is converted
	 */
	static final String SOURCE_NAME = "source";

	/**
	 * Number of nano-seconds in a milli-second
	 */
	static final long NANOS_PER_MILLI_SECOND = 1_000_000;

	/**
	 * Number of nano-seconds in a milli-second
	 */
	static final int MILLIS_PER_SECOND = 1_000;

	/**
	 * The time zone offset retrieved via {@link Tools#getPreferredTimeZone()}. Is updated via a listener whenever the
	 * time zone changes.
	 */
	private static int timeZoneOffset;

	static {
		// register listener that updates time zone offset
		ParameterService.registerParameterChangeListener(new ParameterChangeListener() {
			@Override
			public void informParameterChanged(String key, String value) {
				if (RapidMiner.PROPERTY_RAPIDMINER_GENERAL_TIME_ZONE.equals(key)) {
					timeZoneOffset = Tools.getPreferredTimeZone().getRawOffset();
				}
			}

			@Override
			public void informParameterSaved() {
				//ignore
			}
		});

		timeZoneOffset = Tools.getPreferredTimeZone().getRawOffset();
	}

	// Suppress default constructor for noninstantiability
	private BeltConverter() {
		throw new AssertionError();
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
	public static IOTable convert(ExampleSet exampleSet, ConcurrencyContext context) {
		return ToTableConverter.convert(exampleSet, context);
	}

	/**
	 * Extracts a {@link HeaderExampleSet} from a table. This is useful for creating a {@link
	 * com.rapidminer.example.set.RemappedExampleSet} or specifying training header of a {@link
	 * com.rapidminer.operator.Model}. Should only be used if the model has been trained with the table and the mapping
	 * indices must stay the same in the header. Otherwise wrap with
	 * {@link TableViewCreator#convertOnWriteView(IOTable, boolean)} and use {@link ExampleSet} methods from there on.
	 *
	 * @param table
	 * 		the table to extract from
	 * @return a {@link HeaderExampleSet} where the nominal mappings of the attributes are immutable
	 * @throws ConversionException
	 * 		if the table cannot be converted because it contains non-standard columns
	 */
	public static HeaderExampleSet convertHeader(Table table) {
		return FromTableConverter.convertHeader(table);
	}

	/**
	 * Converts a belt {@link IOTable} into an {@link ExampleSet}. Use the faster
	 * {@link TableViewCreator#convertOnWriteView(IOTable, boolean)} instead if you are not planning change values in
	 * currently existing columns.
	 *
	 * @param tableObject
	 * 		the table object to convert
	 * @param context
	 * 		the context to use for parallel execution
	 * @return a new example set containing the values of the table
	 * @throws IllegalArgumentException
	 * 		if table or context is null
	 * @throws ConversionException
	 * 		if the table cannot be converted because it contains non-standard columns
	 */
	public static ExampleSet convert(IOTable tableObject, ConcurrencyContext context) {
		return FromTableConverter.convert(tableObject, context);
	}

	/**
	 * Converts a table object into an example set sequentially in case no operator is known. If possible, {@link
	 * #convert(IOTable, ConcurrencyContext)} should be preferred. Use the faster
	 * {@link TableViewCreator#convertOnWriteView(IOTable, boolean)} if you are not planning change values in currently
	 * existing columns.
	 *
	 * @param tableObject
	 * 		the table object to convert
	 * @return the example set
	 * @throws ConversionException
	 * 		if the table cannot be converted because it contains non-standard columns
	 */
	public static ExampleSet convertSequentially(IOTable tableObject) {
		return FromTableConverter.convertSequentially(tableObject);
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
			case SOURCE:
				convertedRole = SOURCE_NAME;
				break;
			case ENCODING:
				convertedRole = ENCODING_NAME;
				break;
			case INTERPRETATION:
				convertedRole = INTERPRETATION_NAME;
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
	 * Finds the right {@link Ontology} for a given {@link Column}
	 *
	 * @param column
	 * 		the column to convert
	 * @param columnName
	 * 		the name of the column, used for exceptions
	 * @return the associated ontology
	 * @throws ConversionException
	 * 		if the column cannot be converted because it is a non-standard column
	 */
	public static int convertToOntology(Column column, String columnName) {
		switch (column.type().id()) {
			case INTEGER_53_BIT:
				return Ontology.INTEGER;
			case REAL:
				return Ontology.REAL;
			case NOMINAL:
				Dictionary dictionary = column.getDictionary();
				if (dictionary.isBoolean() && !(dictionary.hasPositive() && !dictionary.hasNegative())) {
					return Ontology.BINOMINAL;
				}
				return Ontology.NOMINAL;
			case DATE_TIME:
				return Ontology.DATE_TIME;
			case TIME:
				return Ontology.TIME;
			default:
				throw new ConversionException(columnName, column.type());
		}
	}

	/**
	 * Checks if the {@link com.rapidminer.example.table.ExampleTable} of the given {@link ExampleSet} wraps a belt
	 * {@link Table}. In this case the performance may differ, in particular {@link ExampleSet#getExample(int)} might be
	 * slower than otherwise.
	 *
	 * @param exampleSet
	 * 		the {@link ExampleSet} to check
	 * @return {@code true} iff the example table of the example set is on top of a {@link Table}
	 */
	public static boolean isTableWrapper(ExampleSet exampleSet) {
		return ToTableConverter.getExampleTable(exampleSet) instanceof ConvertOnWriteExampleTable;
	}

	/**
	 * Converts belt roles to studio roles and adds them to the given Attributes. Duplicate roles will be made
	 * unique by adding an index to them.
	 */
	static void convertRoles(Table table, Attributes allAttributes) {
		// this map is used in case there are duplicate roles to get indices for the duplicate roles
		Map<String, Integer> nextRoleIndex = new HashMap<>();
		for (String label : table.labels()) {
			String studioRole = convertRole(table, label);
			if (studioRole != null) {
				// add an index if necessary
				String studioRoleWithIndex = studioRole;
				while (!checkUnique(allAttributes, studioRoleWithIndex)) {
					int index = nextRoleIndex.getOrDefault(studioRole, 2);
					studioRoleWithIndex = studioRole + "_" + index;
					nextRoleIndex.put(studioRole, index + 1);
				}
				allAttributes.setSpecialAttribute(allAttributes.get(label), studioRoleWithIndex);
			}
		}
	}

	/**
	 * Belt meta data (except for roles) cannot be stored in an ExampleSet. Therefore, we store the belt meta data in
	 * the ExampleSets's user data.
	 *
	 * @param table
	 * 		the table holding the belt meta data that will be stored
	 * @param set
	 * 		the belt meta data will be stored to this ExampleSet's user data
	 */
	static void storeBeltMetaDataInExampleSetUserData(Table table, ExampleSet set) {
		set.setUserData(IOOBJECT_USER_DATA_COLUMN_META_DATA_KEY, Collections.unmodifiableMap(table.getMetaData()));
	}

	/**
	 * Gets the value type from the meta data if present or from the table otherwise.
	 *
	 * @throws ConversionException
	 * 		if the column cannot be converted because it is a non-standard column
	 */
	static int getValueType(Table table, String label, int columnIndex) {
		Column column = table.column(columnIndex);
		int derivedOntology = convertToOntology(column, label);
		LegacyType legacyType = table.getFirstMetaData(label, LegacyType.class);
		if (legacyType != null) {
			int legacyOntology = legacyType.ontology();
			if (useLegacyOntology(legacyOntology, derivedOntology, column)) {
				return legacyOntology;
			}
		}
		return derivedOntology;
	}


	/**
	 * Converts attribute roles into belt column roles.
	 *
	 * @param studioRole
	 * 		the studio role name
	 * @return the appropriate {@link ColumnRole}
	 */
	public static ColumnRole convertRole(String studioRole) {
		String withOutIndex = removeIndex(studioRole);
		switch (withOutIndex) {
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
			case SOURCE_NAME:
				return ColumnRole.SOURCE;
			case ENCODING_NAME:
				return ColumnRole.ENCODING;
			case INTERPRETATION_NAME:
				return ColumnRole.INTERPRETATION;
			default:
				if (withOutIndex.startsWith(Attributes.CONFIDENCE_NAME)) {
					return ColumnRole.SCORE;
				}
				return ColumnRole.METADATA;
		}
	}

	/**
	 * Converts the {@link ColumnRole} to the legacy studio role. The inverse of {@link #convertRole(String)}. In
	 * contrast to {@link #convertRole(Table, String)} it does not take {@link LegacyRole}s into account.
	 *
	 * @param role
	 * 		the role to convert
	 * @return the legacy studio role
	 */
	public static String toStudioRole(ColumnRole role) {
		if (role == null) {
			// Nothing to convert, abort...
			return null;
		}
		switch (role) {
			case LABEL:
				return Attributes.LABEL_NAME;
			case ID:
				return Attributes.ID_NAME;
			case PREDICTION:
				return Attributes.PREDICTION_NAME;
			case SCORE:
				return Attributes.CONFIDENCE_NAME;
			case CLUSTER:
				return Attributes.CLUSTER_NAME;
			case OUTLIER:
				return Attributes.OUTLIER_NAME;
			case WEIGHT:
				return Attributes.WEIGHT_NAME;
			case BATCH:
				return Attributes.BATCH_NAME;
			case SOURCE:
				return SOURCE_NAME;
			case ENCODING:
				return ENCODING_NAME;
			case INTERPRETATION:
				return INTERPRETATION_NAME;
			case METADATA:
				return META_DATA_NAME;
			default:
				return role.name().toLowerCase(Locale.ENGLISH);
		}
	}

	/**
	 * Converts this instant to the number of milliseconds from the epoch of 1970-01-01T00:00:00Z.
	 *
	 * Same as {@code (double)instant.toEpochMilli()} but without the arithmetic exception on overflow. Instead
	 * precision is lost for dates in the far future or past.
	 *
	 * @param instant
	 * 		the instant to convert
	 * @return the epoch milliseconds as double
	 * @since 0.9
	 */
	public static double toEpochMilli(Instant instant) {
		double seconds = instant.getEpochSecond();
		int nanos = instant.getNano();
		if (seconds < 0 && nanos > 0) {
			double millis = (seconds + 1) * MILLIS_PER_SECOND;
			long adjustment = nanos / NANOS_PER_MILLI_SECOND - MILLIS_PER_SECOND;
			return millis + adjustment;
		} else {
			double millis = seconds * MILLIS_PER_SECOND;
			long adjustment = nanos / NANOS_PER_MILLI_SECOND;
			return millis + adjustment;
		}
	}

	/**
	 * Converts the nanoseconds of the day from belt time to the milliseconds of the day in legacy time. Subtracts the
	 * time zone offset of ({@link Tools#getPreferredTimeZone()}) because it will be added again later for the legacy
	 * time.
	 *
	 * @param nanos
	 * 		the nanoseconds of the day
	 * @return milliseconds in the old time format
	 */
	public static double nanoOfDayToLegacyTime(long nanos) {
		long millisOfDay = Math.floorDiv(nanos, BeltConverter.NANOS_PER_MILLI_SECOND);
		return (double) millisOfDay - timeZoneOffset;
	}

	/**
	 * Converts the milliseconds from the legacy time format to the belt time format in nanoseconds of the day. The
	 * milliseconds must not be {@code NaN}.
	 *
	 * @param legacyTime
	 * 		the double value representing the milliseconds since epoch (must not be {@code NaN}).
	 * @param calendar
	 * 		the calendar holding the time zone information needed to do the transformation. Please note that the method
	 * 		modifies the given calendar via {@link Calendar#setTimeInMillis(long)}). Use {@link
	 *        Tools#getPreferredCalendar()} if you want to invert the transformation done in {@link
	 *        #nanoOfDayToLegacyTime(long)}.
	 * @return local time in nanoseconds of the day
	 */
	public static long legacyTimeDoubleToNanoOfDay(double legacyTime, Calendar calendar) {
		calendar.setTimeInMillis((long) legacyTime);
		return (calendar.get(Calendar.MILLISECOND)
				+ (calendar.get(Calendar.SECOND)
				+ (calendar.get(Calendar.MINUTE)
				+ calendar.get(Calendar.HOUR_OF_DAY) * 60) * 60) * MILLIS_PER_SECOND) * NANOS_PER_MILLI_SECOND;
	}

	/**
	 * If the given String ends with an index suffix this suffix is removed.
	 */
	private static String removeIndex(String string) {
		Matcher m = INDEX_PATTERN.matcher(string);
		if (m.matches()) {
			return m.group(1);
		}
		return string;
	}


	/**
	 * Checks if the role has already been set.
	 */
	private static boolean checkUnique(Attributes allAttributes, String studioRole) {
		return allAttributes.findRoleBySpecialName(studioRole) == null;
	}

	/**
	 * Checks if the legacy type should be used instead of the given type.
	 */
	private static boolean useLegacyOntology(int legacyOntology, int derivedOntology, Column column) {
		// we never want to fall back to the legacy ontology for these two
		if (derivedOntology == Ontology.INTEGER || derivedOntology == Ontology.BINOMINAL) {
			return false;
		}
		// legacy ontology is super type or the same
		if (Ontology.ATTRIBUTE_VALUE_TYPE.isA(derivedOntology, legacyOntology)) {
			// time is very different from date-time in belt so we do not allow conversion
			return derivedOntology != Ontology.TIME;
		}
		// if binominal is requested for a nominal derived type, check dictionary size and if only positive
		if (legacyOntology == Ontology.BINOMINAL && derivedOntology == Ontology.NOMINAL) {
			Dictionary dictionary = column.getDictionary();
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

}
