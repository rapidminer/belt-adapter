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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.Statistics;
import com.rapidminer.example.set.AbstractExampleSet;


/**
 * An {@link AbstractExampleSet} that does the statistics calculation row-wise. Used by {@link DoubleTableWrapper}
 * and {@link DatetimeTableWrapper} for which column-wise reading is very slow.
 *
 * @author Gisa Meier
 */
abstract class RowwiseStatisticsExampleSet extends AbstractExampleSet {

	private static final long serialVersionUID = 4312636208688929326L;

	private final Map<String, List<Statistics>> statisticsMap = new HashMap<>();

	@Override
	public void recalculateAllAttributeStatistics() {
		List<Attribute> allAttributes = new ArrayList<>();
		Iterator<Attribute> a = getAttributes().allAttributes();
		while (a.hasNext()) {
			allAttributes.add(a.next());
		}
		recalculateAttributeStatisticsRowWise(allAttributes);
	}

	@Override
	public void recalculateAttributeStatistics(Attribute attribute) {
		List<Attribute> allAttributes = new ArrayList<>();
		allAttributes.add(attribute);
		recalculateAttributeStatisticsRowWise(allAttributes);
	}

	/**
	 * Here the Example Set is parsed only once, all the information is retained for each example set. In contrast to
	 * the method in the super class, this works row-wise.
	 * <p>
	 * The statistics calculation is stopped by {@link Thread#interrupt()}.
	 */
	private synchronized void recalculateAttributeStatisticsRowWise(List<Attribute> attributeList) {
		// do nothing if not desired
		if (attributeList.isEmpty()) {
			return;
		} else {
			// init statistics
			resetAttributeStatisticsForRowwise(attributeList);

			// calculate statistics
			Attribute weightAttribute = getAttributes().getWeight();
			if (weightAttribute != null && !weightAttribute.isNumerical()) {
				weightAttribute = null;
			}

			for (Example example : this) {
				if (weightAttribute == null) {
					for (Attribute attribute : attributeList) {
						double value = example.getValue(attribute);
						attribute.getAllStatistics().forEachRemaining(s -> s.count(value, 1.0d));
					}
				} else {
					double weight = example.getValue(weightAttribute);
					for (Attribute attribute : attributeList) {
						double value = example.getValue(attribute);
						attribute.getAllStatistics().forEachRemaining(s -> s.count(value, weight));
					}
				}
				if (Thread.currentThread().isInterrupted()) {
					// statistics is only partly calculated
					resetAttributeStatisticsForRowwise(attributeList);
					return;
				}
			}
		}

		// store cloned statistics
		for (Attribute attribute : attributeList) {
			// do not directly work on the existing List because that might force a
			// ConcurrentModification and the well known Exception
			List<Statistics> tmpStatisticsList = new LinkedList<>();

			Iterator<Statistics> stats = attribute.getAllStatistics();
			while (stats.hasNext()) {
				Statistics statistics = (Statistics) stats.next().clone();
				tmpStatisticsList.add(statistics);
			}
			statisticsMap.put(attribute.getName(), tmpStatisticsList);
			if (Thread.currentThread().isInterrupted()) {
				return;
			}
		}
	}



	/**
	 * Resets the statistics for all attributes from attributeList.
	 *
	 * @param attributeList
	 * 		the attributes for which to reset the statistics
	 */
	private void resetAttributeStatisticsForRowwise(List<Attribute> attributeList) {
		for (Attribute attribute : attributeList) {
			for (Iterator<Statistics> stats = attribute.getAllStatistics(); stats.hasNext(); ) {
				Statistics statistics = stats.next();
				statistics.startCounting(attribute);
			}
		}
	}


	@Override
	public double getStatistics(Attribute attribute, String statisticsName, String statisticsParameter) {
		List<Statistics> statisticsList = statisticsMap.get(attribute.getName());
		if (statisticsList == null) {
			return Double.NaN;
		}

		for (Statistics statistics : statisticsList) {
			if (statistics.handleStatistics(statisticsName)) {
				return statistics.getStatistics(attribute, statisticsName, statisticsParameter);
			}
		}

		return Double.NaN;
	}


}