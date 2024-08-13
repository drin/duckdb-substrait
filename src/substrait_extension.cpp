#define DUCKDB_EXTENSION_MAIN

#include "from_substrait.hpp"
#include "substrait_extension.hpp"
#include "to_substrait.hpp"
#include "plans.hpp"
#include "engine_duckdb.hpp"

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

  //! Container for bound data received from `ToSubstrait` Table Function
  struct ToSubstraitFunctionData : public TableFunctionData {
    ToSubstraitFunctionData() {}

    string query;
    bool   enable_optimizer;
    bool   strict   { false }; // fail the conversion on possible warnings
    bool   finished { false };
  };

  // Helper functions for `to_substrait` table function
  static void ToJsonFunctionInternal(
     ClientContext&               context
    ,ToSubstraitFunctionData&     data
    ,DataChunk&                   output
    ,Connection&                  new_conn
    ,unique_ptr<LogicalOperator>& query_plan
    ,string&                      serialized
  );

  static void ToSubFunctionInternal(
     ClientContext&               context
    ,ToSubstraitFunctionData&     data
    ,DataChunk&                   output
    ,Connection&                  new_conn
    ,unique_ptr<LogicalOperator>& query_plan
    ,string&                      serialized
  );

  static void VerifyJSONRoundtrip(
     unique_ptr<LogicalOperator>& query_plan
    ,Connection&                  con
    ,ToSubstraitFunctionData&     data
    ,const string&                serialized
  );

  static void VerifyBlobRoundtrip(
     unique_ptr<LogicalOperator>& query_plan
    ,Connection&                  con
    ,ToSubstraitFunctionData&     data
    ,const string&                serialized
  );

  static bool GetOptimizationOption( const ClientConfig&                  config
                                    ,const duckdb::named_parameter_map_t& named_params) {

    // First, check if the user has explicitly requested to enable/disable the optimizer
    for (const auto &param : named_params) {
      auto loption = StringUtil::Lower(param.first);
      if (loption == "enable_optimizer") { return BooleanValue::Get(param.second); }
    }

    // Default to the connection-level setting
    return config.enable_optimizer;
  }

  static unique_ptr<ToSubstraitFunctionData> InitToSubstraitFunctionData(const ClientConfig&     config,
                                                                         TableFunctionBindInput& input) {
    auto result = make_uniq<ToSubstraitFunctionData>();

    result->query            = input.inputs[0].ToString();
    result->enable_optimizer = GetOptimizationOption(config, input.named_parameters);

    return result;
  }

  static unique_ptr<FunctionData> ToSubstraitBind(ClientContext& context, TableFunctionBindInput& input,
                                                  vector<LogicalType>& return_types, vector<string>& names) {
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

    auto sub_relation = SubstraitPlanToDuckDBRel(con, serialized, is_json);
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

    auto& actual_col_coll = actual_result->Collection();
    auto& subs_col_coll   = substrait_materialized->Collection();

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

  static DuckDBToSubstrait
  InitPlanExtractor( ClientContext&               context
                    ,ToSubstraitFunctionData&     data
                    ,Connection&                  new_conn
                    ,unique_ptr<LogicalOperator>& query_plan) {
    // The user might want to disable the optimizer of the new connection
    new_conn.context->config.enable_optimizer      = data.enable_optimizer;
    new_conn.context->config.use_replacement_scans = false;

    // We want for sure to disable the internal compression optimizations.
    // These are DuckDB specific, no other system implements these. Also,
    // respect the user's settings if they chose to disable any specific optimizers.

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

  static void
  ToSubFunctionInternal( ClientContext&               context
                        ,ToSubstraitFunctionData&     data
                        ,DataChunk&                   output
                        ,Connection&                  new_conn
                        ,unique_ptr<LogicalOperator>& query_plan
                        ,string&                      serialized) {
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

    if (!context.config.query_verification_enabled) { return; }

    VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);

    // Also run the ToJson path and verify round-trip for that
    DataChunk other_output;
    other_output.Initialize(context, {LogicalType::VARCHAR});
    ToJsonFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
    VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);
  }

  static void
  ToJsonFunctionInternal( ClientContext&               context
                         ,ToSubstraitFunctionData&     data
                         ,DataChunk&                   output
                         ,Connection&                  new_conn
                         ,unique_ptr<LogicalOperator>& query_plan
                         ,string&                      serialized) {
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

    if (!context.config.query_verification_enabled) { return; }

    VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);

    // Also run the ToJson path and verify round-trip for that
    DataChunk other_output;
    other_output.Initialize(context, {LogicalType::BLOB});
    ToSubFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
    VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);
  }

  //! Container for bound data received from `FromSubstrait` Table Function
  struct FromSubstraitFunctionData : public TableFunctionData {
    FromSubstraitFunctionData() = default;

    shared_ptr<Relation>    plan;
    unique_ptr<QueryResult> result;
    unique_ptr<Connection>  conn;
  };

  static unique_ptr<FunctionData> SubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names, bool is_json) {
    if (input.inputs[0].IsNull()) {
      throw BinderException("from_substrait cannot be called with a NULL parameter");
    }

    string serialized = input.inputs[0].GetValueUnsafe<string>();
    auto   result     = make_uniq<FromSubstraitFunctionData>();

    result->conn = make_uniq<Connection>(*context.db);
    result->plan = SubstraitPlanToDuckDBRel(*result->conn, serialized, is_json);
    for (auto &column : result->plan->Columns()) {
      return_types.emplace_back(column.Type());
      names.emplace_back(column.Name());
    }

    return std::move(result);
  }

  static unique_ptr<FunctionData> FromSubstraitBind(ClientContext& context, TableFunctionBindInput& input,
                                                    vector<LogicalType>& return_types, vector<string>& names) {
    return SubstraitBind(context, input, return_types, names, false);
  }

  static unique_ptr<FunctionData>
  FromSubstraitBindJSON( ClientContext&          context
                        ,TableFunctionBindInput& input
                        ,vector<LogicalType>&    return_types
                        ,vector<string>&         names) {
    return SubstraitBind(context, input, return_types, names, true);
  }

  static void FromSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<FromSubstraitFunctionData>();
    if (!data.result) { data.result = data.plan->Execute(); }

    auto result_chunk = data.result->Fetch();
    if (!result_chunk) { return; }

    output.Move(*result_chunk);
  }


  // ------------------------------
  // Supporting functions for Table Function "translate_mohair"

  static unique_ptr<FunctionData>
  BindingFnTranslateMohair( ClientContext&          context
                           ,TableFunctionBindInput& input
                           ,vector<LogicalType>&    return_types
                           ,vector<string>&         names) {
    if (input.inputs[0].IsNull()) {
      throw BinderException("from_substrait cannot be called with a NULL parameter");
    }
    string plan_msg { input.inputs[0].GetValueUnsafe<string>() };

    // Prepare a FunctionData instance to return
    auto fn_data = make_uniq<FnDataSubstraitTranslation>();
    fn_data->translator       = make_uniq<DuckDBTranslator>(context);
    fn_data->sys_plan         = fn_data->translator->TranslatePlanMessage(plan_msg);
    fn_data->plan_data        = std::make_shared<PreparedStatementData>(StatementType::SELECT_STATEMENT);
    fn_data->enable_optimizer = GetOptimizationOption(context.config, input.named_parameters);

    // For us to further build PreparedStatementData
    // (probably affects our ResultCollector)
    for (auto &column : fn_data->sys_plan->engine->Columns()) {
      fn_data->plan_data->types.emplace_back(column.Type());
      fn_data->plan_data->names.emplace_back(column.Name());
    }

    // Set result schema (binding)
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("Physical Plan");

    return std::move(fn_data);
  }

  static void
  TableFnTranslateMohair( ClientContext&      context
                         ,TableFunctionInput& data_p
                         ,DataChunk&          output) {
    auto &fn_data = (FnDataSubstraitTranslation&) *(data_p.bind_data);
    if (fn_data.finished) { return; }

    if (not fn_data.exec_plan) {
      // Convert plan to engine plan
      fn_data.engine_plan = fn_data.translator->TranspilePlanMessage(fn_data.sys_plan);

      // Convert engine plan to execution plan
      fn_data.exec_plan = fn_data.translator->TranslateLogicalPlan(
        fn_data.engine_plan, fn_data.enable_optimizer
      );

      fn_data.finished = true;
    }

    // output.Initialize(context, { LogicalType::VARCHAR }, 1);
    output.SetCardinality(1);
    output.SetValue(0, 0, fn_data.exec_plan->engine->ToString());
  }

  // ------------------------------
  // Supporting functions for Table Function "execute_mohair"

  static unique_ptr<FunctionData>
  BindingFnExecuteMohair( ClientContext&          context
                         ,TableFunctionBindInput& input
                         ,vector<LogicalType>&    return_types
                         ,vector<string>&         names) {
    if (input.inputs[0].IsNull()) {
      throw BinderException("from_substrait cannot be called with a NULL parameter");
    }

    string plan_msg { input.inputs[0].GetValueUnsafe<string>() };

    auto result = make_uniq<FnDataSubstraitExecution>();
    result->translator = make_uniq<DuckDBTranslator>(context);
    result->sys_plan   = result->translator->TranslatePlanMessage(plan_msg);

    for (auto &column : result->sys_plan->engine->Columns()) {
      return_types.emplace_back(column.Type());
      names.emplace_back(column.Name());
    }

    return result;
  }

  static void
  TableFnExecuteMohair( ClientContext&      context
                       ,TableFunctionInput& data_p
                       ,DataChunk&          output) {
    auto& fn_data = (FnDataSubstraitExecution&) *(data_p.bind_data);

    if (!fn_data.result) {
      fn_data.result = fn_data.sys_plan->engine->Execute();
    }

    auto result_chunk = fn_data.result->Fetch();
    if (result_chunk) { output.Move(*result_chunk); }
  }

  // ------------------------------
  // Initializers for Table Functions that implement extension logic

  //! Create a TableFunction, "get_substrait", then register it with the catalog
  void InitializeGetSubstrait(const Connection &con) {
    TableFunction to_sub_func("get_substrait"
      ,{ LogicalType::VARCHAR }
      ,ToSubFunction
      ,ToSubstraitBind
    );

    to_sub_func.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
    to_sub_func.named_parameters["strict"]           = LogicalType::BOOLEAN;
    CreateTableFunctionInfo to_sub_info(to_sub_func);

    auto &catalog = Catalog::GetSystemCatalog(*con.context);
    catalog.CreateTableFunction(*con.context, to_sub_info);
  }

  //! Create a TableFunction, "get_substrait_json", then register it with the catalog
  void InitializeGetSubstraitJSON(const Connection &con) {
    TableFunction get_substrait_json("get_substrait_json"
      ,{ LogicalType::VARCHAR }
      ,ToJsonFunction
      ,ToJsonBind
    );

    get_substrait_json.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
    CreateTableFunctionInfo get_substrait_json_info(get_substrait_json);

    auto &catalog = Catalog::GetSystemCatalog(*con.context);
    catalog.CreateTableFunction(*con.context, get_substrait_json_info);
  }

  //! Create a TableFunction, "translate_mohair", then register it with the catalog
  void InitializeTranslateMohair(Connection &con) {
    TableFunction tablefn_mohair(
       "translate_mohair"
      ,{ LogicalType::BLOB }
      ,TableFnTranslateMohair
      ,BindingFnTranslateMohair
    );

    CreateTableFunctionInfo fninfo_mohair(tablefn_mohair);

    auto &catalog = Catalog::GetSystemCatalog(*(con.context));
    catalog.CreateTableFunction(*(con.context), fninfo_mohair);
  }

  //! Create a TableFunction, "execute_mohair", then register it with the catalog
  void InitializeExecuteMohair(Connection &con) {
    TableFunction tablefn_mohair(
       "execute_mohair"
      ,{ LogicalType::BLOB }
      ,TableFnExecuteMohair
      ,BindingFnExecuteMohair
    );

    CreateTableFunctionInfo fninfo_mohair(tablefn_mohair);

    auto &catalog = Catalog::GetSystemCatalog(*(con.context));
    catalog.CreateTableFunction(*(con.context), fninfo_mohair);
  }

  //! Create a TableFunction, "from_substrait", then register it with the catalog
  void InitializeFromSubstrait(const Connection &con) {
    TableFunction from_sub_func(
       "from_substrait"
      ,{ LogicalType::BLOB }
      ,FromSubFunction
      ,FromSubstraitBind
    );

    CreateTableFunctionInfo from_sub_info(from_sub_func);

    auto &catalog = Catalog::GetSystemCatalog(*con.context);
    catalog.CreateTableFunction(*con.context, from_sub_info);
  }

  //! Create a TableFunction, "from_substrait_json", then register it with the catalog
  void InitializeFromSubstraitJSON(const Connection &con) {
    TableFunction from_sub_func_json(
       "from_substrait_json"
      ,{ LogicalType::VARCHAR }
      ,FromSubFunction
      ,FromSubstraitBindJSON
    );

    CreateTableFunctionInfo from_sub_info_json(from_sub_func_json);

    auto &catalog = Catalog::GetSystemCatalog(*con.context);
    catalog.CreateTableFunction(*con.context, from_sub_info_json);
  }

  //! Logic for loading this extension
  void SubstraitExtension::Load(DuckDB &db) {
    Connection con(db);
    con.BeginTransaction();

    InitializeGetSubstrait(con);
    InitializeGetSubstraitJSON(con);

    InitializeFromSubstrait(con);
    InitializeFromSubstraitJSON(con);
    InitializeTranslateMohair(con);
    InitializeExecuteMohair(con);

    con.Commit();
  }

  std::string SubstraitExtension::Name() {
    return "substrait";
  }

} // namespace duckdb


extern "C" {

  DUCKDB_EXTENSION_API
  void substrait_init(duckdb::DatabaseInstance& db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
  }

  DUCKDB_EXTENSION_API
  const char* substrait_version() {
    return duckdb::DuckDB::LibraryVersion();
  }
}
