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
package org.vanilladb.core.storage.tx.concurrency;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class ReadCommittedConcurrencyMgr extends ConcurrencyMgr {
	
	private List<Object> toReleaseSLockAtEndStatement = new ArrayList<Object>();

	public ReadCommittedConcurrencyMgr(long txNumber) {
		txNum = txNumber;
	}

	@Override
	public void onTxCommit(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxRollback(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	/**
	 * Releases all slocks obtained so far.
	 */
	@Override
	public void onTxEndStatement(Transaction tx) {
		for (Object obj : toReleaseSLockAtEndStatement)
			lockTbl.release(obj, txNum, LockTable.S_LOCK);
	}

	@Override
	public void modifyFile(String fileName) {
		lockTbl.xLock(fileName, txNum);
	}

	@Override
	public void readFile(String fileName) {
		lockTbl.isLock(fileName, txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(fileName, txNum, LockTable.IS_LOCK);
	}

	@Override
	public void insertBlock(BlockId blk) {
		lockTbl.xLock(blk.fileName(), txNum);
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void modifyBlock(BlockId blk) {
		lockTbl.ixLock(blk.fileName(), txNum);
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void readBlock(BlockId blk) {
		lockTbl.isLock(blk.fileName(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(blk.fileName(), txNum, LockTable.IS_LOCK);
		
		lockTbl.sLock(blk, txNum);
		// releases S lock at the end of statement to allow unrepeatable Read
		toReleaseSLockAtEndStatement.add(blk);
	}
	
	@Override
	public void modifyRecord(RecordId recId) {
		lockTbl.ixLock(recId.block().fileName(), txNum);
		lockTbl.ixLock(recId.block(), txNum);
		lockTbl.xLock(recId, txNum);
	}

	@Override
	public void readRecord(RecordId recId) {
		lockTbl.isLock(recId.block().fileName(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(recId.block().fileName(), txNum, LockTable.IS_LOCK);
		
		lockTbl.isLock(recId.block(), txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(recId.block(), txNum, LockTable.IS_LOCK);
		
		lockTbl.sLock(recId, txNum);
		// releases S lock at the end of statement to allow unrepeatable Read
		toReleaseSLockAtEndStatement.add(recId);
	}

	@Override
	public void modifyIndex(String dataFileName) {
		lockTbl.xLock(dataFileName, txNum);
	}

	@Override
	public void readIndex(String dataFileName) {
		lockTbl.isLock(dataFileName, txNum);
		// releases IS lock to allow phantoms
		lockTbl.release(dataFileName, txNum, LockTable.IS_LOCK);
	}
	
	@Override
	public void modifyLeafBlock(BlockId blk) {
		// Hold the index locks until the end of the transaction
		// in order to prevent Serializable transactions from phantoms
		lockTbl.xLock(blk, txNum);
	}
	
	@Override
	public void readLeafBlock(BlockId blk) {
		// releases S lock at the end of statement to allow unrepeatable Read
		toReleaseSLockAtEndStatement.add(blk);
	}
}
