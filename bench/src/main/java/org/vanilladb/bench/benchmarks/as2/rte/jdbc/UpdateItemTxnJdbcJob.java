/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;
import org.vanilladb.bench.server.param.as2.UpdateItemProcParamHelper;

public class UpdateItemTxnJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(UpdateItemTxnJdbcJob.class
			.getName());
	
	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		UpdateItemProcParamHelper paramHelper = new UpdateItemProcParamHelper();
		paramHelper.prepareParameters(pars);
		
		// Output message
		StringBuilder outputMsg = new StringBuilder("[");
		
		// Execute logic
		try {
			Statement statement = conn.createStatement();
			Statement statement_ = conn.createStatement();
			ResultSet rs = null;
			
			// SELECT
			for (int i = 0; i < paramHelper.getUpdateCount(); i++) {
				int iid = paramHelper.getUpdateItemId(i);
				double delta = paramHelper.getUpdateItemValue(i);
				String sql = "SELECT i_price FROM item WHERE i_id = " + iid;
				rs = statement.executeQuery(sql);
				rs.beforeFirst();
				if (rs.next()) {
					double price = rs.getDouble("i_price");
					double newPrice = (price + delta > As2BenchConstants.MAX_PRICE) 
    						? (double)As2BenchConstants.MIN_PRICE : (double)(price + delta);
					
					String sqlUpdate = "UPDATE item SET i_price = " + newPrice + " Where i_id = " +  iid;
					statement_.executeQuery(sqlUpdate);
					outputMsg.append(String.format("The price of item %d has been changed from %f to %f ", iid, price, newPrice));
				} else
					throw new RuntimeException("cannot find the record with i_id = " + iid);
				rs.close();
			}
			
			conn.commit();
		
			
			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
}
