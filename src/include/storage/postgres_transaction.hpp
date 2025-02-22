//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "postgres_connection.hpp"

namespace duckdb {
class PostgresCatalog;
class PostgresSchemaEntry;
class PostgresTableEntry;

enum class PostgresTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class PostgresTransaction : public Transaction {
public:
	PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context);
	~PostgresTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	PostgresConnection &GetConnection();
	unique_ptr<PostgresResult> Query(const string &query);
	static PostgresTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	PostgresConnection connection;
	PostgresTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
