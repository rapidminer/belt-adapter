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

import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.tools.Ontology;


/**
 * A legacy type in the form of a single {@code int} representing an {@link Ontology}. Used to keep track of column
 * types that are not supported by belt.
 *
 * @author Gisa Meier
 */
enum LegacyType implements ColumnMetaData {

	NOMINAL(Ontology.NOMINAL),
	NUMERICAL(Ontology.NUMERICAL),
	INTEGER(Ontology.INTEGER),
	REAL(Ontology.REAL),
	STRING(Ontology.STRING),
	BINOMINAL(Ontology.BINOMINAL),
	POLYNOMINAL(Ontology.POLYNOMINAL),
	FILE_PATH(Ontology.FILE_PATH),
	DATE_TIME(Ontology.DATE_TIME),
	DATE(Ontology.DATE),
	TIME(Ontology.TIME);

	/**
	 * Identifier for column meta data of type legacy role.
	 */
	private static final String TYPE_ID = "com.rapidminer.belt.meta.column.legacy.type";

	private final int ontology;

	LegacyType(int ontology) {
		this.ontology = ontology;
	}

	@Override
	public Uniqueness uniqueness() {
		return Uniqueness.COLUMN;
	}

	/**
	 * Returns the column ontology.
	 *
	 * @return the ontology
	 */
	int ontology() {
		return ontology;
	}

	@Override
	public String type() {
		return TYPE_ID;
	}

	/**
	 * Returns the legacy type for the given ontology. Ontology must be between 1 and 11 (inclusive).
	 */
	static LegacyType forOntology(int ontology) {
		return values()[ontology - 1];
	}

}
