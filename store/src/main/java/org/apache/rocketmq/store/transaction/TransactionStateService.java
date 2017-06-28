/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.store.transaction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;


/**
 * clOffset - Commit Log Offset<br>
 * tsOffset - Transaction State Table Offset
 */
public class TransactionStateService {
	public static final int TSStoreUnitSize = 24;

    public static final String TRANSACTION_REDOLOG_TOPIC = "TRANSACTION_REDOLOG_TOPIC_XXXX";
    public static final int TRANSACTION_REDOLOG_TOPIC_QUEUEID = 0;
    public final static long PreparedMessageTagsCode = -1;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final static int TS_STATE_POS = 20;
    private static final Logger tranlog = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    private final ByteBuffer byteBufferAppend = ByteBuffer.allocate(TSStoreUnitSize);

    private final ConsumeQueue tranRedoLog;

    private final AtomicLong tranStateTableOffset = new AtomicLong(0);

    private final Timer timer = new Timer("CheckTransactionMessageTimer", true);
    private final Map<MappedFile, Boolean> timerExistMap = new ConcurrentHashMap<MappedFile, Boolean>();
    private MappedFileQueue tranStateTable;
    
    private boolean recoverd = false;	//启动后是否已恢复数据文件

    public TransactionStateService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.tranStateTable =
                new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getTranStateTableStorePath(),
                    defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize(), null);

        this.tranRedoLog = new ConsumeQueue(//
            TRANSACTION_REDOLOG_TOPIC,//
            TRANSACTION_REDOLOG_TOPIC_QUEUEID,//
            defaultMessageStore.getMessageStoreConfig().getTranRedoLogStorePath(),//
            defaultMessageStore.getMessageStoreConfig().getTranRedoLogMapedFileSize(),//
            defaultMessageStore);
    }


    public boolean load(final boolean lastExitOK) {
        boolean result = this.tranRedoLog.load();
        result = result && this.tranStateTable.load();
        
        if (!lastExitOK) {
        	//若非正常退出，则清空redoLog和tranStateTable
        	this.tranRedoLog.destroy();
        	this.tranStateTable.destroy();
        }

        return result;
    }

    public void start() {
        this.initTimerTask();
    }

    private void initTimerTask() {
        final List<MappedFile> mappedFiles = this.tranStateTable.getMappedFiles();
        for (MappedFile mf : mappedFiles) {
            this.addTimerTask(mf);
        }
    }


    private void addTimerTask(final MappedFile mf) {
    	Boolean exist = timerExistMap.get(mf);
    	if (exist != null && exist == true) {
    		return;
    	}
    	
        this.timerExistMap.put(mf, true);
        this.timer.scheduleAtFixedRate(new CheckTimerTask(mf), 1000 * 60, this.defaultMessageStore.getMessageStoreConfig()
            .getCheckTransactionMessageTimerInterval());
    }
    
    private final class CheckTimerTask extends TimerTask {
		private final MappedFile mappedFile;
		private final TransactionCheckExecuter transactionCheckExecuter =
		        TransactionStateService.this.defaultMessageStore.getTransactionCheckExecuter();
		private final long checkTransactionMessageAtleastInterval =
		        TransactionStateService.this.defaultMessageStore.getMessageStoreConfig()
		            .getCheckTransactionMessageAtleastInterval();
		private final boolean slave = TransactionStateService.this.defaultMessageStore
		    .getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;

		private CheckTimerTask(MappedFile mf) {
			mappedFile = mf;
		}

		@Override
		public void run() {
		    if (slave)
		        return;

		    if (!TransactionStateService.this.defaultMessageStore.getMessageStoreConfig()
		        .isCheckTransactionMessageEnable()) {
		        return;
		    }

		    try {

		        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
		        if (selectMappedBufferResult != null) {
		            long preparedMessageCountInThisMapedFile = 0;
		            int i = 0;
		            try {

		                for (; i < selectMappedBufferResult.getSize(); i += TSStoreUnitSize) {
		                    selectMappedBufferResult.getByteBuffer().position(i);

		                    // Commit Log Offset
		                    long clOffset = selectMappedBufferResult.getByteBuffer().getLong();
		                    // Message Size
		                    int msgSize = selectMappedBufferResult.getByteBuffer().getInt();
		                    // Timestamp
		                    int timestamp = selectMappedBufferResult.getByteBuffer().getInt();
		                    // Producer Group Hashcode
		                    int groupHashCode = selectMappedBufferResult.getByteBuffer().getInt();
		                    // Transaction State
		                    int tranType = selectMappedBufferResult.getByteBuffer().getInt();
		                    if (tranType != MessageSysFlag.TRANSACTION_PREPARED_TYPE) {
		                        continue;
		                    }

		                    long timestampLong = timestamp * 1000;
		                    long diff = System.currentTimeMillis() - timestampLong;
		                    if (diff < checkTransactionMessageAtleastInterval) {
		                        break;
		                    }

		                    preparedMessageCountInThisMapedFile++;

		                    try {
		                        this.transactionCheckExecuter.gotoCheck(//
		                            groupHashCode,//
		                            getTranStateOffset(i),//
		                            clOffset,//
		                            msgSize);
		                    }
		                    catch (Exception e) {
		                        tranlog.warn("gotoCheck Exception", e);
		                    }
		                }

		                if (0 == preparedMessageCountInThisMapedFile //
		                        && i == mappedFile.getFileSize()) {
		                    tranlog
		                        .info(
		                            "remove the transaction timer task, because no prepared message in this mapedfile[{}]",
		                            mappedFile.getFileName());
		                    this.cancel();
		                    timerExistMap.remove(mappedFile);
		                }
		            }
		            finally {
		                selectMappedBufferResult.release();
		            }

		            tranlog
		                .info(
		                    "the transaction timer task execute over in this period, {} Prepared Message: {} Check Progress: {}/{}",
		                    mappedFile.getFileName(),//
		                    preparedMessageCountInThisMapedFile,//
		                    i / TSStoreUnitSize,//
		                    mappedFile.getFileSize() / TSStoreUnitSize//
		                );
		        }
		        else if (mappedFile.isFull()) {
		            tranlog.info("the mapedfile[{}] maybe deleted, cancel check transaction timer task",
		                mappedFile.getFileName());
		            this.cancel();
                    timerExistMap.remove(mappedFile);
		            return;
		        }
		    }
		    catch (Exception e) {
		        log.error("check transaction timer task Exception", e);
		    }
		}

		private long getTranStateOffset(final long currentIndex) {
		    long offset =
		            (this.mappedFile.getFileFromOffset() + currentIndex)
		                    / TransactionStateService.TSStoreUnitSize;
		    return offset;
		}
	}


    public void shutdown() {
        this.timer.cancel();
    }


    public int deleteExpiredStateFile(long offset) {
        int cnt = this.tranStateTable.deleteExpiredFileByOffset(offset, TSStoreUnitSize);
        return cnt;
    }


    public void recoverStateTable(final boolean lastExitOK) {
        if (lastExitOK) {
            this.recoverStateTableNormal();
        }
        else {
            this.tranStateTable.destroy();
            this.recreateStateTable();
        }
    }

    private void recreateStateTable() {
        this.tranStateTable =
                new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getTranStateTableStorePath(),
                    defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize(), null);

        final TreeSet<Long> preparedItemSet = new TreeSet<Long>();

        final long minOffset = this.tranRedoLog.getMinOffsetInQueue();
        long processOffset = minOffset;
        while (true) {
            SelectMappedBufferResult bufferConsumeQueue = this.tranRedoLog.getIndexBuffer(processOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long i = 0;
                    for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetMsg = bufferConsumeQueue.getByteBuffer().getLong();
                        int sizeMsg = bufferConsumeQueue.getByteBuffer().getInt();
                        long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                        //跳过空白的CQ记录
                        if (offsetMsg == 0L && sizeMsg == 0L && tagsCode == 0L) {
                        	continue;
                        }
                        
                        // Prepared
                        if (TransactionStateService.PreparedMessageTagsCode == tagsCode) {
                            preparedItemSet.add(offsetMsg);
                        }
                        // Commit/Rollback
                        else {
                            preparedItemSet.remove(tagsCode);
                        }
                    }

                    processOffset += i;
                }
                finally {
                    bufferConsumeQueue.release();
                }
            }
            else {
                break;
            }
        }

        log.info("scan transaction redolog over, End offset: {},  Prepared Transaction Count: {}",
            processOffset, preparedItemSet.size());

        Iterator<Long> it = preparedItemSet.iterator();
        while (it.hasNext()) {
            Long offset = it.next();

            MessageExt msgExt = this.defaultMessageStore.lookMessageByOffset(offset);
            if (msgExt != null) {

                this.appendPreparedTransaction(msgExt.getCommitLogOffset(), msgExt.getStoreSize(),
                    (int) (msgExt.getStoreTimestamp() / 1000),
                    msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode());
                this.tranStateTableOffset.incrementAndGet();
            }
        }
        
        recoverd = true;
    }

    public boolean appendPreparedTransaction(//
            final long clOffset,//
            final int size,//
            final int timestamp,//
            final int groupHashCode//
    ) {
        //[Zhaobin],和3.5.8相比，MappedFileQueue.getLastMappedFile()函数的语义发生了变化，不会自动创建文件。
    	//带startOffset参数的getLastMappedFile函数能够自动创建文件
    	//MappedFile mappedFile = this.tranStateTable.getLastMappedFile();
        MappedFile mappedFile = this.tranStateTable.getLastMappedFile(0L);
        if (null == mappedFile) {
            log.error("appendPreparedTransaction: create mapedfile error.");
            return false;
        }

        if (recoverd && 0 == mappedFile.getWrotePosition()) {
            this.addTimerTask(mappedFile);
        }

        this.byteBufferAppend.position(0);
        this.byteBufferAppend.limit(TSStoreUnitSize);

        // Commit Log Offset
        this.byteBufferAppend.putLong(clOffset);
        // Message Size
        this.byteBufferAppend.putInt(size);
        // Timestamp
        this.byteBufferAppend.putInt(timestamp);
        // Producer Group Hashcode
        this.byteBufferAppend.putInt(groupHashCode);
        // Transaction State
        this.byteBufferAppend.putInt(MessageSysFlag.TRANSACTION_PREPARED_TYPE);

        return mappedFile.appendMessage(this.byteBufferAppend.array());
    }


    private void recoverStateTableNormal() {

        final List<MappedFile> mappedFiles = this.tranStateTable.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.tranStateTable.getMappedFileSize();
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            /*
             * queue offset(global offset)
             */
            long processOffset = mappedFile.getFileFromOffset();
            /*
             * file offset(local offset)
             */
            long mapedFileOffset = 0;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += TSStoreUnitSize) {

                    final long clOffset_read = byteBuffer.getLong();
                    final int size_read = byteBuffer.getInt();
                    final int timestamp_read = byteBuffer.getInt();
                    final int groupHashCode_read = byteBuffer.getInt();
                    /**
                     * prepared/commit/rollback
                     */
                    final int state_read = byteBuffer.getInt();

                    boolean stateOK = false;
                    switch (state_read) {
                    case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                    case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                        stateOK = true;
                        break;
                    default:
                        break;
                    }

                    if (clOffset_read >= 0 && size_read > 0 && stateOK) {
                        mapedFileOffset = i + TSStoreUnitSize;
                    }
                    else {
                        log.info("recover current transaction state table file over,  "
                                + mappedFile.getFileName() + " " + clOffset_read + " " + size_read + " "
                                + timestamp_read);
                        break;
                    }
                }

                if (mapedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        log.info("recover last transaction state table file over, last maped file "
                                + mappedFile.getFileName());
                        break;
                    }
                    else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();

                        processOffset = mappedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next transaction state table file, " + mappedFile.getFileName());
                    }
                }
                else {
                    log.info("recover current transaction state table queue over " + mappedFile.getFileName()
                            + " " + (processOffset + mapedFileOffset));
                    break;
                }
            }

            processOffset += mapedFileOffset;

            this.tranStateTable.truncateDirtyFiles(processOffset);

            this.tranStateTableOffset.set(this.tranStateTable.getMaxOffset() / TSStoreUnitSize);
            log.info("recover normal over, transaction state table max offset: {}",
                this.tranStateTableOffset.get());
            recoverd = true;
        }
    }

    public boolean updateTransactionState(//
            final long tsOffset,//
            final long clOffset,//
            final int groupHashCode,//
            final int state//
    ) {
        SelectMappedBufferResult selectMappedBufferResult = this.findTransactionBuffer(tsOffset);
        if (selectMappedBufferResult != null) {
            try {
                final long clOffset_read = selectMappedBufferResult.getByteBuffer().getLong();
                final int size_read = selectMappedBufferResult.getByteBuffer().getInt();
                final int timestamp_read = selectMappedBufferResult.getByteBuffer().getInt();
                final int groupHashCode_read = selectMappedBufferResult.getByteBuffer().getInt();
                final int state_read = selectMappedBufferResult.getByteBuffer().getInt();

                if (clOffset != clOffset_read) {
                    log.error("updateTransactionState error clOffset: {} clOffset_read: {}", clOffset,
                        clOffset_read);
                    return false;
                }

                if (groupHashCode != groupHashCode_read) {
                    log.error("updateTransactionState error groupHashCode: {} groupHashCode_read: {}",
                        groupHashCode, groupHashCode_read);
                    return false;
                }

                if (MessageSysFlag.TRANSACTION_PREPARED_TYPE != state_read) {
                    log.warn("updateTransactionState error, the transaction is updated before.");
                    return true;
                }

                selectMappedBufferResult.getByteBuffer().putInt(TS_STATE_POS, state);
            }
            catch (Exception e) {
                log.error("updateTransactionState exception", e);
            }
            finally {
                selectMappedBufferResult.release();
            }
        }

        return false;
    }


    private SelectMappedBufferResult findTransactionBuffer(final long tsOffset) {
        final int mapedFileSize =
                this.defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize();
        final long offset = tsOffset * TSStoreUnitSize;
        MappedFile mappedFile = this.tranStateTable.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mapedFileSize));
            return result;
        }

        return null;
    }


    public AtomicLong getTranStateTableOffset() {
        return tranStateTableOffset;
    }


    public ConsumeQueue getTranRedoLog() {
        return tranRedoLog;
    }


	public boolean isRecoverd() {
		return recoverd;
	}


	public void setRecoverd(boolean recoverd) {
		this.recoverd = recoverd;
	}
}
