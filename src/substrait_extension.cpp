#define DUCKDB_EXTENSION_MAIN

#include "from_substrait.hpp"
#include "substrait_extension.hpp"
#include "to_substrait.hpp"
#include "plans.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#endif

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
  ToSubstraitFunctionData() = default;
  string query;
  bool   enable_optimizer { false };

  //! We will fail the conversion on possible warnings
  bool   strict   { false };
  bool   finished { false };
};

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);
static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);
static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);

static void SetOptions( ToSubstraitFunctionData&     function
                       ,const ClientConfig&          config
                       ,const named_parameter_map_t& named_params) {
  bool optimizer_option_set = false;

  for (const auto &param : named_params) {
    auto loption = StringUtil::Lower(param.first);
    // If the user has explicitly requested to enable/disable the optimizer when
    // generating Substrait, then that takes precedence.
    if (loption == "enable_optimizer") {
      function.enable_optimizer = BooleanValue::Get(param.second);
      optimizer_option_set      = true;
    }

    if (loption == "strict") {
      function.strict = BooleanValue::Get(param.second);
    }
  }

  if (!optimizer_option_set) {
    // If the user has not specified what they want, fall back to the settings
    // on the connection (e.g. if the optimizer was disabled by the user at
    // the connection level, it would be surprising to enable the optimizer
    // when generating Substrait).
    function.enable_optimizer = config.enable_optimizer;
  }
}

static unique_ptr<ToSubstraitFunctionData> InitToSubstraitFunctionData(const ClientConfig &config,
                                                                       TableFunctionBindInput &input) {
  auto result = make_uniq<ToSubstraitFunctionData>();
  result->query = input.inputs[0].ToString();
  SetOptions(*result, config, input.named_parameters);
  return result;
}

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
  return_types.emplace_back(LogicalType::BLOB);
  names.emplace_back("Plan Blob");
  return InitToSubstraitFunctionData(context.config, input);
}

static unique_ptr<FunctionData> ToJsonBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("Json");
  return InitToSubstraitFunctionData(context.config, input);
}

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, const string &serialized, bool json = false) {
  SubstraitToDuckDB transformer_s2d(conn, serialized, json);
  return transformer_s2d.TransformPlan();
}

static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con,
                                     ToSubstraitFunctionData &data, const string &serialized, bool is_json) {
  // We round-trip the generated json and verify if the result is the same
  auto actual_result = con.Query(data.query);
  auto sub_relation  = SubstraitPlanToDuckDBRel(con, serialized, is_json);

  auto substrait_result = sub_relation->Execute();
  substrait_result->names = actual_result->names;
  unique_ptr<MaterializedQueryResult> substrait_materialized;

  if (substrait_result->type == QueryResultType::STREAM_RESULT) {
    auto &stream_query = substrait_result->Cast<StreamQueryResult>();

    substrait_materialized = stream_query.Materialize();
  }

  else if (substrait_result->type == QueryResultType::MATERIALIZED_RESULT) {
    substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
  }

  auto &actual_col_coll = actual_result->Collection();
  auto &subs_col_coll   = substrait_materialized->Collection();
  string error_message;

  if (!ColumnDataCollection::ResultEquals(actual_col_coll, subs_col_coll, error_message)) {
    query_plan->Print();
    sub_relation->Print();
    actual_col_coll.Print();
    subs_col_coll.Print();
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
  disabled_optimizers.insert(OptimizerType::MATERIALIZED_CTE);
  DBConfig::GetConfig(*new_conn.context).options.disabled_optimizers = disabled_optimizers;

  query_plan = new_conn.context->ExtractPlan(data.query);
  return DuckDBToSubstrait(context, *query_plan, data.strict);
}

static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
  output.SetCardinality(1);
  auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
  serialized = transformer_d2s.SerializeToString();
  output.SetValue(0, 0, Value::BLOB_RAW(serialized));
}

static void ToSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &data = data_p.bind_data->CastNoConst<ToSubstraitFunctionData>();
  if (data.finished) { return; }

  auto new_conn = Connection(*context.db);
	// If error(varchar) gets implemented in substrait this can be removed
	new_conn.Query("SET scalar_subquery_error_on_multiple_rows=false;");

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
  auto &data = data_p.bind_data->CastNoConst<ToSubstraitFunctionData>();
  if (data.finished) { return; }

  auto new_conn = Connection(*context.db);
	// If error(varchar) gets implemented in substrait this can be removed
	new_conn.Query("SET scalar_subquery_error_on_multiple_rows=false;");

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

  unique_ptr<DuckDBTranslator>      translator;
  shared_ptr<Relation>              plan_rel;
  unique_ptr<LogicalOperator>       logical_plan;
  shared_ptr<PreparedStatementData> plan_data;
  unique_ptr<QueryResult>           res;

  bool is_optimized { false };
};

struct ExecMohairFunctionData : public TableFunctionData {
  ExecMohairFunctionData() = default;

  unique_ptr<DuckDBTranslator> translator;
  shared_ptr<Relation>         exec_plan;
  unique_ptr<QueryResult>      res;
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

  auto fn_data        = make_uniq<OptimizeMohairFunctionData>();
  fn_data->translator = make_uniq<DuckDBTranslator>(context);
  fn_data->plan_data  = make_shared<PreparedStatementData>(StatementType::SELECT_STATEMENT);

  string plan_msg { input.inputs[0].GetValueUnsafe<string>() };
  fn_data->plan_rel     = fn_data->translator->TranslatePlanMessage(plan_msg);
  fn_data->logical_plan = fn_data->translator->TranspilePlanRel(fn_data->plan_rel);

  for (auto &column : fn_data->plan_rel->Columns()) {
    // For duckdb framework to do something with
    return_types.emplace_back(column.Type());
    names.emplace_back(column.Name());

    // For us to further build PreparedStatementData
    // (probably affects our ResultCollector)
    fn_data->plan_data->types.emplace_back(column.Type());
    fn_data->plan_data->names.emplace_back(column.Name());
  }

  return std::move(fn_data);
}

static unique_ptr<FunctionData>
BindingTranslateMohair( ClientContext&          context
                       ,TableFunctionBindInput& input
                       ,vector<LogicalType>&    return_types
                       ,vector<string>&         names) {

  if (input.inputs[0].IsNull()) {
    throw BinderException("from_substrait cannot be called with a NULL parameter");
  }

  string plan_msg { input.inputs[0].GetValueUnsafe<string>() };
  auto   result   = make_uniq<ExecMohairFunctionData>();

  result->translator = make_uniq<DuckDBTranslator>(context);
  result->exec_plan  = result->translator->TranslatePlanMessage(plan_msg);

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
  auto &data = data_p.bind_data->CastNoConst<FromSubstraitFunctionData>();
  if (!data.res) { data.res = data.plan->Execute(); }

  auto result_chunk = data.res->Fetch();
  if (!result_chunk) { return; }

  output.Move(*result_chunk);
}

static void
TableFnOptimizeMohair( ClientContext&      context
                      ,TableFunctionInput& data_p
                      ,DataChunk&          output) {
  auto &fn_data = (OptimizeMohairFunctionData &) *(data_p.bind_data);

  if (!fn_data.res) {
    shared_ptr<Binder> binder = Binder::CreateBinder(context);

    // >> Optimization phase
    std::cout << "[Optimization] >>" << std::endl;
    std::cout << "\t[Optimizer]: declare" << std::endl;
    Optimizer optimizer { *binder, context };

    std::cout << "\t[Optimizer]: optimize" << std::endl;
    fn_data.is_optimized = true;
    fn_data.logical_plan = optimizer.Optimize(std::move(fn_data.logical_plan));

    // >> Planning phase
    std::cout << "[Planning] >>" << std::endl;
    std::cout << "\t[Planner]: declare" << std::endl;
    PhysicalPlanGenerator physical_planner { context };

    std::cout << "\t[Planner]: create physical plan" << std::endl;
    fn_data.plan_data->plan = physical_planner.CreatePlan(
      std::move(fn_data.logical_plan)
    );

    std::cout << "\t[Planner]: define collector" << std::endl;
    auto result_collector = PhysicalResultCollector::GetResultCollector(
      context, *(fn_data.plan_data)
    );

    // >> Execution phase
    std::cout << "[Execution] >>" << std::endl;
    constexpr bool dry_run       { false };
    Executor       plan_executor { context };

    std::cout << "\t[Executor]: initialize" << std::endl;
    plan_executor.Initialize(std::move(result_collector));

    std::cout << "\t[Executor]: execute first task" << std::endl;
    auto exec_result = plan_executor.ExecuteTask(dry_run);

    std::cout << "\t[Executor]: execute remaining tasks" << std::endl;
    while (exec_result != PendingExecutionResult::RESULT_READY) {
      switch (exec_result) {
        case PendingExecutionResult::RESULT_NOT_READY:
        case PendingExecutionResult::RESULT_READY:
          break;

        case PendingExecutionResult::BLOCKED:
          std::cout << "\t[Executor]: blocked" << std::endl;
          break;

        case PendingExecutionResult::NO_TASKS_AVAILABLE:
          std::cout << "\t[Executor]: waiting for tasks" << std::endl;
          break;

        case PendingExecutionResult::EXECUTION_ERROR:
          std::cerr << "\t[Executor]: execution error: "
                    << plan_executor.GetError().Message()
                    << std::endl
          ;
          return;

        default:
          std::cerr << "\t[Executor]: unknown result" << std::endl;
          return;
      }

      exec_result = plan_executor.ExecuteTask(dry_run);
    }

    std::cout << "\t[Executor]: checking for result" << std::endl;
    if (exec_result == PendingExecutionResult::RESULT_READY) {

      if (plan_executor.HasResultCollector()) {
        std::cout << "\t[Executor]: gather result" << std::endl;
        fn_data.res = std::move(plan_executor.GetResult());
      }
      else {
        std::cerr << "\t[Executor]: missing result collector" << std::endl;
        return;
      }
    }
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

void InitializeGetSubstrait(const Connection &con) {
  auto &catalog = Catalog::GetSystemCatalog(*con.context);

  // create the get_substrait table function that allows us to get a substrait
  // binary from a valid SQL Query
  TableFunction to_sub_func("get_substrait", {LogicalType::VARCHAR}, ToSubFunction, ToSubstraitBind);
  to_sub_func.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
  to_sub_func.named_parameters["strict"]           = LogicalType::BOOLEAN;
  CreateTableFunctionInfo to_sub_info(to_sub_func);
  catalog.CreateTableFunction(*con.context, to_sub_info);
}

void InitializeGetSubstraitJSON(const Connection &con) {
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

void InitializeFromSubstrait(const Connection &con) {
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

void InitializeFromSubstraitJSON(const Connection &con) {
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
