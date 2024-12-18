//===----------------------------------------------------------------------===//
//                         DuckDB
//
// to_substrait.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "custom_extensions/custom_extensions.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "skyproto/substrait/algebra.pb.h"
#include "skyproto/substrait/plan.pb.h"
#include <string>
#include <unordered_map>

namespace skysubstrait = skyproto::substrait;

namespace duckdb {
class DuckDBToSubstrait {
public:
	explicit DuckDBToSubstrait(ClientContext &context, LogicalOperator &dop, bool strict_p)
	    : context(context), strict(strict_p) {
		TransformPlan(dop);
	};

	~DuckDBToSubstrait() {
		plan.Clear();
	}
	//! Serializes the substrait plan to a string
	string SerializeToString() const;
	string SerializeToJson() const;

private:
	//! Transform DuckDB Plan to Substrait Plan
	void TransformPlan(LogicalOperator &dop);
	//! Registers a function
	uint64_t RegisterFunction(const std::string &name, vector<skysubstrait::Type> &args_types);
	//! Creates a reference to a table column
	static void CreateFieldRef(skysubstrait::Expression *expr, uint64_t col_idx);
	//! In case of struct types we might we do DFS to get all names
	static vector<string> DepthFirstNames(const LogicalType &type);
	static void DepthFirstNamesRecurse(vector<string> &names, const LogicalType &type);
	static skysubstrait::Expression_Literal ToExpressionLiteral(const skysubstrait::Expression &expr);
	static void SetTableSchema(const TableCatalogEntry &table, skysubstrait::NamedStruct *schema);
	static void SetNamedTable(const TableCatalogEntry &table, skysubstrait::WriteRel *writeRel);

	//! Transforms Relation Root
	skysubstrait::RelRoot *TransformRootOp(LogicalOperator &dop);

	//! Methods to Transform Logical Operators to Substrait Relations
	skysubstrait::Rel *TransformOp(LogicalOperator &dop);
	skysubstrait::Rel *TransformFilter(LogicalOperator &dop);
	skysubstrait::Rel *TransformProjection(LogicalOperator &dop);
	skysubstrait::Rel *TransformTopN(LogicalOperator &dop);
	skysubstrait::Rel *TransformLimit(LogicalOperator &dop);
	skysubstrait::Rel *TransformOrderBy(LogicalOperator &dop);
	skysubstrait::Rel *TransformComparisonJoin(LogicalOperator &dop);
	skysubstrait::Rel *TransformAggregateGroup(LogicalOperator &dop);
	skysubstrait::Rel *TransformExpressionGet(LogicalOperator &dop);
	skysubstrait::Rel *TransformGet(LogicalOperator &dop);
	skysubstrait::Rel *TransformCrossProduct(LogicalOperator &dop);
	skysubstrait::Rel *TransformUnion(LogicalOperator &dop);
	skysubstrait::Rel *TransformDistinct(LogicalOperator &dop);
	skysubstrait::Rel *TransformExcept(LogicalOperator &dop);
	skysubstrait::Rel *TransformIntersect(LogicalOperator &dop);
	skysubstrait::Rel *TransformCreateTable(LogicalOperator &dop);
	skysubstrait::Rel *TransformInsertTable(LogicalOperator &dop);
	skysubstrait::Rel *TransformDeleteTable(LogicalOperator &dop);

	static skysubstrait::Rel *TransformDummyScan();
	static vector<LogicalType>::size_type GetColumnCount(LogicalOperator &dop);
	static skysubstrait::RelCommon *CreateOutputMapping(vector<int32_t> vector);
	static bool IsPassthroughProjection(LogicalProjection &dproj, idx_t child_column_count, bool &needs_output_mapping);

	//! Methods to transform different LogicalGet Types (e.g., Table, Parquet)
	//! To Substrait;
	void TransformTableScanToSubstrait(LogicalGet &dget, skysubstrait::ReadRel *sget) const;
	void TransformParquetScanToSubstrait(LogicalGet &dget, skysubstrait::ReadRel *sget, BindInfo &bind_info,
	                                     const FunctionData &bind_data) const;

	//! Methods to transform DuckDBConstants to Substrait Expressions
	static void TransformConstant(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformInteger(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformDouble(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformBigInt(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformDate(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformVarchar(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformBoolean(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformDecimal(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformHugeInt(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformSmallInt(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformFloat(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformTime(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformInterval(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformTimestamp(const Value &dval, skysubstrait::Expression &sexpr);
	static void TransformEnum(const Value &dval, skysubstrait::Expression &sexpr);

	//! Methods to transform a DuckDB Expression to a Substrait Expression
	void TransformExpr(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset = 0);
	static void TransformBoundRefExpression(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset);
	void TransformCastExpression(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset);
	void TransformFunctionExpression(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset);
	static void TransformConstantExpression(Expression &dexpr, skysubstrait::Expression &sexpr);
	void TransformComparisonExpression(Expression &dexpr, skysubstrait::Expression &sexpr);
	void TransformBetweenExpression(Expression &dexpr, skysubstrait::Expression &sexpr);
	void TransformConjunctionExpression(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset);
	void TransformNotNullExpression(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset);
	void TransformIsNullExpression(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset);
	void TransformNotExpression(Expression &dexpr, skysubstrait::Expression &sexpr, uint64_t col_offset);
	void TransformCaseExpression(Expression &dexpr, skysubstrait::Expression &sexpr);
	void TransformInExpression(Expression &dexpr, skysubstrait::Expression &sexpr);

	//! Transforms a DuckDB Logical Type into a Substrait Type
	static skysubstrait::Type DuckToSubstraitType(const LogicalType &type, BaseStatistics *column_statistics = nullptr,
	                                           bool not_null = false);

	//! Methods to transform DuckDB Filters to Substrait Expression
	skysubstrait::Expression *TransformFilter(uint64_t col_idx, LogicalType &column_type, TableFilter &dfilter,
	                                       LogicalType &return_type);
	skysubstrait::Expression *TransformIsNotNullFilter(uint64_t col_idx, const LogicalType &column_type,
	                                                TableFilter &dfilter, const LogicalType &return_type);
	skysubstrait::Expression *TransformConjuctionAndFilter(uint64_t col_idx, LogicalType &column_type,
	                                                    TableFilter &dfilter, LogicalType &return_type);
	skysubstrait::Expression *TransformConstantComparisonFilter(uint64_t col_idx, const LogicalType &column_type,
	                                                         TableFilter &dfilter, const LogicalType &return_type);

	//! Transforms DuckDB Join Conditions to Substrait Expression
	skysubstrait::Expression *TransformJoinCond(const JoinCondition &dcond, uint64_t left_ncol);
	//! Transforms DuckDB Sort Order to Substrait Sort Order
	void TransformOrder(const BoundOrderByNode &dordf, skysubstrait::SortField &sordf);

	static void AllocateFunctionArgument(skysubstrait::Expression_ScalarFunction *scalar_fun,
	                                     skysubstrait::Expression *value);
	static std::string &RemapFunctionName(std::string &function_name);
	static bool IsExtractFunction(const string &function_name);

	//! Creates a Conjunction
	template <typename T, typename FUNC>
	skysubstrait::Expression *CreateConjunction(T &source, const FUNC f) {
		skysubstrait::Expression *res = nullptr;
		for (auto &ele : source) {
			auto child_expression = f(ele);
			if (!res) {
				res = child_expression;
			} else {

				auto temp_expr = new skysubstrait::Expression();
				auto scalar_fun = temp_expr->mutable_scalar_function();
				LogicalType boolean_type(LogicalTypeId::BOOLEAN);

				vector<skysubstrait::Type> args_types {DuckToSubstraitType(boolean_type),
				                                       DuckToSubstraitType(boolean_type)};

				scalar_fun->set_function_reference(RegisterFunction("and", args_types));
				*scalar_fun->mutable_output_type() = DuckToSubstraitType(boolean_type);
				AllocateFunctionArgument(scalar_fun, res);
				AllocateFunctionArgument(scalar_fun, child_expression);
				res = temp_expr;
			}
		}
		return res;
	}

	//! Variables used to register functions
	unordered_map<string, uint64_t> functions_map;
	unordered_map<string, uint64_t> extension_uri_map;

	//! Remapped DuckDB functions names to Substrait compatible function names
	static const unordered_map<std::string, std::string> function_names_remap;
	static const case_insensitive_set_t valid_extract_subfields;
	//! Variable that holds information about yaml function extensions
	static const SubstraitCustomFunctions custom_functions;
	uint64_t last_function_id = 1;
	uint64_t last_uri_id = 1;
	//! The substrait Plan
	skysubstrait::Plan plan;
	ClientContext &context;
	//! If we are generating a query plan on strict mode we will error if
	//! things don't go perfectly shiny
	bool strict;
	string errors;
};
} // namespace duckdb
