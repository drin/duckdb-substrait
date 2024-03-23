#define DUCKDB_EXTENSION_MAIN

#include "from_substrait.hpp"
#include "substrait_extension.hpp"
#include "to_substrait.hpp"
#include "plans.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#endif

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
	ToSubstraitFunctionData() {
	}
	string query;
	bool enable_optimizer;
	bool finished = false;
};

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);
static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);
static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);

static bool SetOptimizationOption(const ClientConfig &config, const duckdb::named_parameter_map_t &named_params) {
	for (const auto &param : named_params) {
		auto loption = StringUtil::Lower(param.first);
		// If the user has explicitly requested to enable/disable the optimizer when
		// generating Substrait, then that takes precedence.
		if (loption == "enable_optimizer") {
			return BooleanValue::Get(param.second);
		}
	}

	// If the user has not specified what they want, fall back to the settings
	// on the connection (e.g. if the optimizer was disabled by the user at
	// the connection level, it would be surprising to enable the optimizer
	// when generating Substrait).
	return config.enable_optimizer;
}

static unique_ptr<ToSubstraitFunctionData> InitToSubstraitFunctionData(const ClientConfig &config,
                                                                       TableFunctionBindInput &input) {
	auto result = make_uniq<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	result->enable_optimizer = SetOptimizationOption(config, input.named_parameters);
	return std::move(result);
}

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	auto result = InitToSubstraitFunctionData(context.config, input);
	return std::move(result);
}

static unique_ptr<FunctionData> ToJsonBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Json");
	auto result = InitToSubstraitFunctionData(context.config, input);
	return std::move(result);
}

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, const string &serialized, bool json = false) {
	SubstraitToDuckDB transformer_s2d(conn, serialized, json);
	return transformer_s2d.TransformPlan();
}

static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con,
                                     ToSubstraitFunctionData &data, const string &serialized, bool is_json) {
	// We round-trip the generated json and verify if the result is the same
	auto actual_result = con.Query(data.query);

	auto sub_relation = SubstraitPlanToDuckDBRel(con, serialized, is_json);
	auto substrait_result = sub_relation->Execute();
	substrait_result->names = actual_result->names;
	unique_ptr<MaterializedQueryResult> substrait_materialized;

	if (substrait_result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_query = substrait_result->Cast<duckdb::StreamQueryResult>();

		substrait_materialized = stream_query.Materialize();
	} else if (substrait_result->type == QueryResultType::MATERIALIZED_RESULT) {
		substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
	}
	auto actual_col_coll = actual_result->Collection();
	auto subs_col_coll = substrait_materialized->Collection();
	string error_message;
	if (!ColumnDataCollection::ResultEquals(actual_col_coll, subs_col_coll, error_message)) {
		query_plan->Print();
		sub_relation->Print();
		throw InternalException("The query result of DuckDB's query plan does not match Substrait : " + error_message);
	}
}

static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, con, data, serialized, false);
}

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, con, data, serialized, true);
}

static DuckDBToSubstrait InitPlanExtractor(ClientContext &context, ToSubstraitFunctionData &data, Connection &new_conn,
                                           unique_ptr<LogicalOperator> &query_plan) {
	// The user might want to disable the optimizer of the new connection
	new_conn.context->config.enable_optimizer = data.enable_optimizer;
	new_conn.context->config.use_replacement_scans = false;

	// We want for sure to disable the internal compression optimizations.
	// These are DuckDB specific, no other system implements these. Also,
	// respect the user's settings if they chose to disable any specific optimizers.
	//
	// The InClauseRewriter optimization converts large `IN` clauses to a
	// "mark join" against a `ColumnDataCollection`, which may not make
	// sense in other systems and would complicate the conversion to Substrait.
	set<OptimizerType> disabled_optimizers = DBConfig::GetConfig(context).options.disabled_optimizers;
	disabled_optimizers.insert(OptimizerType::IN_CLAUSE);
	disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
	DBConfig::GetConfig(*new_conn.context).options.disabled_optimizers = disabled_optimizers;

	query_plan = new_conn.context->ExtractPlan(data.query);
	return DuckDBToSubstrait(context, *query_plan);
}

static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
	serialized = transformer_d2s.SerializeToString();
	output.SetValue(0, 0, Value::BLOB_RAW(serialized));
}

static void ToSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToSubFunctionInternal(context, data, output, new_conn, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::VARCHAR});
	ToJsonFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
	VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);
}

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
	serialized = transformer_d2s.SerializeToJson();
	output.SetValue(0, 0, serialized);
}

static void ToJsonFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToJsonFunctionInternal(context, data, output, new_conn, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::BLOB});
	ToSubFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
	VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);
}

struct FromSubstraitFunctionData : public TableFunctionData {
	FromSubstraitFunctionData() = default;
	shared_ptr<Relation>    plan;
	unique_ptr<QueryResult> res;
	unique_ptr<Connection>  conn;
};

struct OptimizeMohairFunctionData : public TableFunctionData {
	OptimizeMohairFunctionData() = default;
	unique_ptr<LogicalOperator>  logical_plan;
  unique_ptr<PhysicalOperator> physical_plan;
	unique_ptr<QueryResult>      res;

  bool is_optimized { false };
  bool is_physical  { false };
};

struct ExecMohairFunctionData : public TableFunctionData {
	ExecMohairFunctionData() = default;
	shared_ptr<Relation>    exec_plan;
	unique_ptr<QueryResult> res;
};

static unique_ptr<FunctionData> SubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names, bool is_json) {
	auto result = make_uniq<FromSubstraitFunctionData>();
	result->conn = make_uniq<Connection>(*context.db);
	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	result->plan = SubstraitPlanToDuckDBRel(*result->conn, serialized, is_json);
	for (auto &column : result->plan->Columns()) {
		return_types.emplace_back(column.Type());
		names.emplace_back(column.Name());
	}
	return std::move(result);
}


static unique_ptr<FunctionData>
BindingTranspileMohair( ClientContext&          context
           ,TableFunctionBindInput& input
           ,vector<LogicalType>&    return_types
           ,vector<string>&         names) {

	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}

  DuckDBTranslator translator { context };
	string           plan_msg   { input.inputs[0].GetValueUnsafe<string>() };

	auto result          = make_uniq<OptimizeMohairFunctionData>();
	result->logical_plan = translator.TranspilePlanMessage(plan_msg);

	for (auto &column : result->logical_plan->Columns()) {
		return_types.emplace_back(column.Type());
		names.emplace_back(column.Name());
	}

	return std::move(result);
}

static unique_ptr<FunctionData>
BindingTranslateMohair( ClientContext&          context
           ,TableFunctionBindInput& input
           ,vector<LogicalType>&    return_types
           ,vector<string>&         names) {

	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}

  DuckDBTranslator translator { context };
	string           plan_msg   { input.inputs[0].GetValueUnsafe<string>() };

	auto result       = make_uniq<ExecMohairFunctionData>();
	result->exec_plan = translator.TranslatePlanMessage(plan_msg);

	for (auto &column : result->exec_plan->Columns()) {
		return_types.emplace_back(column.Type());
		names.emplace_back(column.Name());
	}

	return std::move(result);
}

static unique_ptr<FunctionData>
FromSubstraitBind( ClientContext&          context
                  ,TableFunctionBindInput& input
                  ,vector<LogicalType>&    return_types
                  ,vector<string>&         names) {
	return SubstraitBind(context, input, return_types, names, false);
}

static unique_ptr<FunctionData> FromSubstraitBindJSON(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return SubstraitBind(context, input, return_types, names, true);
}

static void FromSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (FromSubstraitFunctionData &)*data_p.bind_data;
	if (!data.res) {
		data.res = data.plan->Execute();
	}
	auto result_chunk = data.res->Fetch();
	if (!result_chunk) {
		return;
	}
	output.Move(*result_chunk);
}

static void
TableFnOptimizeMohair( ClientContext&      context
                      ,TableFunctionInput& data_p
                      ,DataChunk&          output) {
	auto &fn_data = (OptimizeMohairFunctionData &) *(data_p.bind_data);
	if (!fn_data.res) {
    std::cout << "optimize plan" << std::endl;
    shared_ptr<Binder> binder = Binder::CreateBinder(context);
    Optimizer          optimizer { *binder, context };

    fn_data.logical_plan = optimizer.Optimize(std::move(fn_data.logical_plan));
    fn_data.is_optimized = true;

    std::cout << "translate logical plan to physical plan" << std::endl;
    PhysicalPlanGenerator physical_planner { context };
    fn_data.physical_plan = physical_planner.CreatePlan(std::move(fn_data.logical_plan));
    fn_data.is_physical   = true;

    // TODO
    std::cout << "execute physical plan" << std::endl;

    fn_data.res = fn_data.logical_plan
  }

	auto result_chunk = fn_data.res->Fetch();
	if (!result_chunk) { return; }

	output.Move(*result_chunk);
}

static void
TableFnExecuteMohair( ClientContext&      context
                     ,TableFunctionInput& data_p
                     ,DataChunk&          output) {
	auto &fn_data = (ExecMohairFunctionData &) *(data_p.bind_data);
	if (!fn_data.res) { fn_data.res = fn_data.exec_plan->Execute(); }

	auto result_chunk = fn_data.res->Fetch();
	if (!result_chunk) { return; }

	output.Move(*result_chunk);
}

void InitializeGetSubstrait(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait
	// binary from a valid SQL Query
	TableFunction to_sub_func("get_substrait", {LogicalType::VARCHAR}, ToSubFunction, ToSubstraitBind);
	to_sub_func.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo to_sub_info(to_sub_func);
	catalog.CreateTableFunction(*con.context, to_sub_info);
}

void InitializeGetSubstraitJSON(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait
	// JSON from a valid SQL Query
	TableFunction get_substrait_json("get_substrait_json", {LogicalType::VARCHAR}, ToJsonFunction, ToJsonBind);

	get_substrait_json.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo get_substrait_json_info(get_substrait_json);
	catalog.CreateTableFunction(*con.context, get_substrait_json_info);
}

void InitializeTranspileMohair(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*(con.context));

	// create the from_mohair table function
	TableFunction tablefn_mohair(
     "transpile_mohair"
    ,{ LogicalType::BLOB }
    ,TableFnOptimizeMohair
    ,BindingTranspileMohair
  );

	CreateTableFunctionInfo fninfo_mohair(tablefn_mohair);
	catalog.CreateTableFunction(*(con.context), fninfo_mohair);
}

void InitializeTranslateMohair(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*(con.context));

	// create the from_mohair table function
	TableFunction tablefn_mohair(
     "execute_mohair"
    ,{ LogicalType::BLOB }
    ,TableFnExecuteMohair
    ,BindingTranslateMohair
  );

	CreateTableFunctionInfo fninfo_mohair(tablefn_mohair);
	catalog.CreateTableFunction(*(con.context), fninfo_mohair);
}

void InitializeFromSubstrait(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func(
     "from_substrait"
    ,{ LogicalType::BLOB }
    ,FromSubFunction
    ,FromSubstraitBind
  );

	CreateTableFunctionInfo from_sub_info(from_sub_func);
	catalog.CreateTableFunction(*con.context, from_sub_info);
}

void InitializeFromSubstraitJSON(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func_json("from_substrait_json", {LogicalType::VARCHAR}, FromSubFunction,
	                                 FromSubstraitBindJSON);
	CreateTableFunctionInfo from_sub_info_json(from_sub_func_json);
	catalog.CreateTableFunction(*con.context, from_sub_info_json);
}

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	InitializeGetSubstrait(con);
	InitializeGetSubstraitJSON(con);

	InitializeFromSubstrait(con);
	InitializeFromSubstraitJSON(con);
  InitializeTranspileMohair(con);
  InitializeTranslateMohair(con);

	con.Commit();
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
