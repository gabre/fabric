/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package ledger

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/openblockchain/obc-peer/openchain/db"
	"github.com/openblockchain/obc-peer/openchain/util"
	"github.com/openblockchain/obc-peer/protos"
	"github.com/tecbot/gorocksdb"
	"golang.org/x/net/context"
)

// Blockchain holds basic information in memory. Operations on Blockchain are not thread-safe
// TODO synchronize access to in-memory variables
type blockchain struct {
	size               uint64
	previousBlockHash  []byte
	indexer            blockchainIndexer
	lastProcessedBlock *lastProcessedBlock
}

type lastProcessedBlock struct {
	block       *protos.Block
	blockNumber uint64
	blockHash   []byte
}

var indexBlockDataSynchronously = true

func newBlockchain() (*blockchain, error) {
	size, err := fetchBlockchainSizeFromDB()
	if err != nil {
		return nil, err
	}
	blockchain := &blockchain{0, nil, nil, nil}
	blockchain.size = size
	if size > 0 {
		previousBlock, err := fetchBlockFromDB(size - 1)
		if err != nil {
			return nil, err
		}
		previousBlockHash, err := previousBlock.GetHash()
		if err != nil {
			return nil, err
		}
		blockchain.previousBlockHash = previousBlockHash
	}

	err = blockchain.startIndexer()
	if err != nil {
		return nil, err
	}
	return blockchain, nil
}

func (blockchain *blockchain) startIndexer() (err error) {
	if indexBlockDataSynchronously {
		blockchain.indexer = newBlockchainIndexerSync()
	} else {
		blockchain.indexer = newBlockchainIndexerAsync()
	}
	err = blockchain.indexer.start(blockchain)
	return
}

// getLastBlock get last block in blockchain
func (blockchain *blockchain) getLastBlock() (*protos.Block, error) {
	if blockchain.size == 0 {
		return nil, nil
	}
	return blockchain.getBlock(blockchain.size - 1)
}

// getSize number of blocks in blockchain
func (blockchain *blockchain) getSize() uint64 {
	return blockchain.size
}

// getBlock get block at arbitrary height in block chain
func (blockchain *blockchain) getBlock(blockNumber uint64) (*protos.Block, error) {
	return fetchBlockFromDB(blockNumber)
}

// getBlockByHash get block by block hash
func (blockchain *blockchain) getBlockByHash(blockHash []byte) (*protos.Block, error) {
	blockNumber, err := blockchain.indexer.fetchBlockNumberByBlockHash(blockHash)
	if err != nil {
		return nil, err
	}
	return blockchain.getBlock(blockNumber)
}

func (blockchain *blockchain) getTransactionByUUID(txUUID string) (*protos.Transaction, error) {
	blockNumber, txIndex, err := blockchain.indexer.fetchTransactionIndexByUUID(txUUID)
	if err != nil {
		return nil, err
	}
	block, err := blockchain.getBlock(blockNumber)
	if err != nil {
		return nil, err
	}
	transaction := block.GetTransactions()[txIndex]
	return transaction, nil
}

// getTransactions get all transactions in a block identified by block number
func (blockchain *blockchain) getTransactions(blockNumber uint64) ([]*protos.Transaction, error) {
	block, err := blockchain.getBlock(blockNumber)
	if err != nil {
		return nil, err
	}
	return block.GetTransactions(), nil
}

// getTransactionsByBlockHash get all transactions in a block identified by block hash
func (blockchain *blockchain) getTransactionsByBlockHash(blockHash []byte) ([]*protos.Transaction, error) {
	block, err := blockchain.getBlockByHash(blockHash)
	if err != nil {
		return nil, err
	}
	return block.GetTransactions(), nil
}

// getTransaction get a transaction identified by blocknumber and index within the block
func (blockchain *blockchain) getTransaction(blockNumber uint64, txIndex uint64) (*protos.Transaction, error) {
	block, err := blockchain.getBlock(blockNumber)
	if err != nil {
		return nil, err
	}
	return block.GetTransactions()[txIndex], nil
}

// getTransactionByBlockHash get a transaction identified by blockhash and index within the block
func (blockchain *blockchain) getTransactionByBlockHash(blockHash []byte, txIndex uint64) (*protos.Transaction, error) {
	block, err := blockchain.getBlockByHash(blockHash)
	if err != nil {
		return nil, err
	}
	return block.GetTransactions()[txIndex], nil
}

func (blockchain *blockchain) getBlockchainInfo() (*protos.BlockchainInfo, error) {
	if blockchain.getSize() == 0 {
		return &protos.BlockchainInfo{Height: 0}, nil
	}

	lastBlock, err := blockchain.getLastBlock()
	if err != nil {
		return nil, err
	}

	info := &protos.BlockchainInfo{
		Height:            blockchain.getSize(),
		CurrentBlockHash:  blockchain.previousBlockHash,
		PreviousBlockHash: lastBlock.PreviousBlockHash}

	return info, nil
}

func (blockchain *blockchain) addPersistenceChangesForNewBlock(ctx context.Context,
	block *protos.Block, stateHash []byte, writeBatch *gorocksdb.WriteBatch) (uint64, error) {
	block.SetPreviousBlockHash(blockchain.previousBlockHash)
	block.StateHash = stateHash
	if block.NonHashData == nil {
		block.NonHashData = &protos.NonHashData{LocalLedgerCommitTimestamp: util.CreateUtcTimestamp()}
	} else {
		block.NonHashData.LocalLedgerCommitTimestamp = util.CreateUtcTimestamp()
	}
	blockNumber := blockchain.size
	blockHash, err := block.GetHash()
	if err != nil {
		return 0, err
	}
	blockBytes, blockBytesErr := block.Bytes()
	if blockBytesErr != nil {
		return 0, blockBytesErr
	}
	writeBatch.PutCF(db.GetDBHandle().BlockchainCF, encodeBlockNumberDBKey(blockNumber), blockBytes)
	writeBatch.PutCF(db.GetDBHandle().BlockchainCF, blockCountKey, encodeUint64(blockNumber+1))
	if blockchain.indexer.isSynchronous() {
		blockchain.indexer.createIndexesSync(block, blockNumber, blockHash, writeBatch)
	}
	blockchain.lastProcessedBlock = &lastProcessedBlock{block, blockNumber, blockHash}
	return blockNumber, nil
}

func (blockchain *blockchain) blockPersistenceStatus(success bool) {
	if success {
		blockchain.size++
		blockchain.previousBlockHash = blockchain.lastProcessedBlock.blockHash
		if !blockchain.indexer.isSynchronous() {
			blockchain.indexer.createIndexesAsync(blockchain.lastProcessedBlock.block,
				blockchain.lastProcessedBlock.blockNumber, blockchain.lastProcessedBlock.blockHash)
		}
	}
	blockchain.lastProcessedBlock = nil
}

func (blockchain *blockchain) persistRawBlock(block *protos.Block, blockNumber uint64) error {
	blockBytes, blockBytesErr := block.Bytes()
	if blockBytesErr != nil {
		return blockBytesErr
	}
	writeBatch := gorocksdb.NewWriteBatch()
	writeBatch.PutCF(db.GetDBHandle().BlockchainCF, encodeBlockNumberDBKey(blockNumber), blockBytes)

	// Need to check as we suport out of order blocks in cases such as block/state synchronization. This is
	// really blockchain height, not size.
	if blockchain.getSize() < blockNumber+1 {
		sizeBytes := encodeUint64(blockNumber + 1)
		writeBatch.PutCF(db.GetDBHandle().BlockchainCF, blockCountKey, sizeBytes)
	}
	blockHash, err := block.GetHash()
	if err != nil {
		return err
	}

	if blockchain.indexer.isSynchronous() {
		blockchain.indexer.createIndexesSync(block, blockNumber, blockHash, writeBatch)
	}

	opt := gorocksdb.NewDefaultWriteOptions()
	err = db.GetDBHandle().DB.Write(opt, writeBatch)
	if err != nil {
		return err
	}
	return nil
}

// addBlock add a new block to blockchain
// func (blockchain *blockchain) addBlock(ctx context.Context, block *protos.Block) error {
// 	block.SetPreviousBlockHash(blockchain.previousBlockHash)
// 	state := getState()
// 	stateHash, err := state.getHash()
// 	if err != nil {
// 		return err
// 	}
// 	block.StateHash = stateHash
// 	currentBlockNumber := blockchain.size
// 	currentBlockHash, err := block.GetHash()
// 	if err != nil {
// 		return err
// 	}
// 	err = blockchain.persistBlock(block, currentBlockNumber, currentBlockHash, true)
// 	if err != nil {
// 		return err
// 	}
// 	blockchain.size++
// 	blockchain.previousBlockHash = currentBlockHash
// 	state.clearInMemoryChanges()
// 	if !blockchain.indexer.isSynchronous() {
// 		blockchain.indexer.createIndexesAsync(block, currentBlockNumber, currentBlockHash)
// 	}
// 	return nil
// }

func fetchBlockFromDB(blockNumber uint64) (*protos.Block, error) {
	blockBytes, err := db.GetDBHandle().GetFromBlockchainCF(encodeBlockNumberDBKey(blockNumber))
	if err != nil {
		return nil, err
	}
	if blockBytes == nil {
		return nil, nil
	}
	return protos.UnmarshallBlock(blockBytes)
}

func fetchTransactionFromDB(blockNum uint64, txIndex uint64) (*protos.Transaction, error) {
	block, err := fetchBlockFromDB(blockNum)
	if err != nil {
		return nil, err
	}
	return block.GetTransactions()[txIndex], nil
}

func fetchBlockchainSizeFromDB() (uint64, error) {
	bytes, err := db.GetDBHandle().GetFromBlockchainCF(blockCountKey)
	if err != nil {
		return 0, err
	}
	if bytes == nil {
		return 0, nil
	}
	return decodeToUint64(bytes), nil
}

func fetchBlockchainSizeFromSnapshot(snapshot *gorocksdb.Snapshot) (uint64, error) {
	blockNumberBytes, err := db.GetDBHandle().GetFromBlockchainCFSnapshot(snapshot, blockCountKey)
	if err != nil {
		return 0, err
	}
	var blockNumber uint64
	if blockNumberBytes != nil {
		blockNumber = decodeToUint64(blockNumberBytes)
	}
	return blockNumber, nil
}

var blockCountKey = []byte("blockCount")

func encodeBlockNumberDBKey(blockNumber uint64) []byte {
	return encodeUint64(blockNumber)
}

func decodeBlockNumberDBKey(dbKey []byte) uint64 {
	return decodeToUint64(dbKey)
}

func encodeUint64(number uint64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, number)
	return bytes
}

func decodeToUint64(bytes []byte) uint64 {
	return binary.BigEndian.Uint64(bytes)
}

func (blockchain *blockchain) String() string {
	var buffer bytes.Buffer
	size := blockchain.getSize()
	for i := uint64(0); i < size; i++ {
		block, blockErr := blockchain.getBlock(i)
		if blockErr != nil {
			return ""
		}
		buffer.WriteString("\n----------<block #")
		buffer.WriteString(strconv.FormatUint(i, 10))
		buffer.WriteString(">----------\n")
		buffer.WriteString(block.String())
		buffer.WriteString("\n----------<\\block #")
		buffer.WriteString(strconv.FormatUint(i, 10))
		buffer.WriteString(">----------\n")
	}
	return buffer.String()
}
