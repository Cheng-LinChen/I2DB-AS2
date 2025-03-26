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
package org.vanilladb.bench.server.procedure.as2;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.server.param.as2.UpdateItemProcParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class UpdateItemTxnProc extends StoredProcedure<UpdateItemProcParamHelper> {

	public UpdateItemTxnProc() {
		super(new UpdateItemProcParamHelper());
	}

	@Override
	protected void executeSql() {
		UpdateItemProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();
		
		// SELECT
		for (int idx = 0; idx < paramHelper.getUpdateCount(); idx++) {
			int iid = paramHelper.getUpdateItemId(idx);
			double delta = paramHelper.getUpdateItemValue(idx);
			Scan s = StoredProcedureHelper.executeQuery(
				"SELECT i_name, i_price FROM item WHERE i_id = " + iid,
				tx
			);
			s.beforeFirst();
			if (s.next()) {
				String name = (String) s.getVal("i_name").asJavaVal();
				double price = (Double) s.getVal("i_price").asJavaVal();
				double newPrice = (price + delta > As2BenchConstants.MAX_PRICE) 
    					? As2BenchConstants.MIN_PRICE : (price + delta);

				paramHelper.setItemName(name, idx);
				paramHelper.setItemPrice(newPrice, idx);

				StoredProcedureHelper.executeUpdate(
					"UPDATE item SET i_price = " + newPrice + " WHERE i_id = " + iid,
					tx
				);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);

			s.close();
		}
	}
}
