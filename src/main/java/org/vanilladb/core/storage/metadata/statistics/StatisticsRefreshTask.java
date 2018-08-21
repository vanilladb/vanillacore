/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.metadata.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.storage.tx.Transaction;

public class StatisticsRefreshTask extends Task {
	private static Logger logger = Logger.getLogger(StatisticsRefreshTask.class
			.getName());

	private List<String> refreshtbls;
	private Transaction tx;

	public StatisticsRefreshTask(Transaction tx, String... tblNames) {
		this.tx = tx;

		this.refreshtbls = new ArrayList<String>();
		for (int i = 0; i < tblNames.length; i++)
			this.refreshtbls.add(tblNames[i]);
	}

	@Override
	public void run() {
		if (logger.isLoggable(Level.FINE))
			logger.info("Start refreshing statistics of table");
		while (!refreshtbls.isEmpty())
			VanillaDb.statMgr().refreshStatistics(refreshtbls.remove(0),
					this.tx);
	}
}
