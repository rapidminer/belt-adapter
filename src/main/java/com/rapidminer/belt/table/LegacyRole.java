/**
 * Copyright (C) 2001-2019 by RapidMiner and the contributors
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

import java.util.Objects;

import com.rapidminer.belt.util.ColumnMetaData;


/**
 * A legacy role in the form of a single {@code String}. There are no restrictions on the content of the role other than
 * being non-null. Used to keep track of role names that are not supported by belt.
 *
 * @author Gisa Meier
 */
class LegacyRole implements ColumnMetaData {

	/**
	 * Identifier for column meta data of type legacy role.
	 */
	private static final String TYPE_ID = "com.rapidminer.belt.meta.column.legacy.role";

	private static final int TO_STRING_CUTOFF = 50;

	private final String role;

	LegacyRole(String role) {
		this.role = Objects.requireNonNull(role, "Role must not be null");
	}

	@Override
	public Uniqueness uniqueness() {
		return Uniqueness.COLUMN;
	}

	/**
	 * Returns the column role.
	 *
	 * @return the role
	 */
	String role() {
		return role;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || getClass() != other.getClass()) {
			return false;
		}
		LegacyRole that = (LegacyRole) other;
		return Objects.equals(role, that.role);
	}

	@Override
	public int hashCode() {
		return 197 * role.hashCode();
	}

	@Override
	public String toString() {
		if (role.length() > TO_STRING_CUTOFF) {
			return role.substring(0, TO_STRING_CUTOFF - 3) + "...";
		} else {
			return role;
		}
	}

	@Override
	public String type() {
		return TYPE_ID;
	}

}
