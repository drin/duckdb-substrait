#pragma once

#include <string>
#include <unordered_map>
#include "skytether/substrait/plan.pb.h"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace skysubstrait = skytether::substrait;


namespace duckdb {

class SubstraitToDuckDB {
public:
	SubstraitToDuckDB(Connection &con_p, const string &serialized, bool json = false);
	//! Transforms Substrait Plan to DuckDB Relation
	shared_ptr<Relation> TransformPlan();

private:
	//! Transforms Substrait Plan Root To a DuckDB Relation
	shared_ptr<Relation> TransformRootOp(const skysubstrait::RelRoot &sop);
	//! Transform Substrait Operations to DuckDB Relations
	shared_ptr<Relation> TransformOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformJoinOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformCrossProductOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformFetchOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformFilterOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformProjectOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformAggregateOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformReadOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformSortOp(const skysubstrait::Rel &sop);
	shared_ptr<Relation> TransformSetOp(const skysubstrait::Rel &sop);

	//! Transform Substrait Expressions to DuckDB Expressions
	unique_ptr<ParsedExpression> TransformExpr(const skysubstrait::Expression &sexpr);
	static unique_ptr<ParsedExpression> TransformLiteralExpr(const skysubstrait::Expression &sexpr);
	static unique_ptr<ParsedExpression> TransformSelectionExpr(const skysubstrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformScalarFunctionExpr(const skysubstrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformIfThenExpr(const skysubstrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformCastExpr(const skysubstrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformInExpr(const skysubstrait::Expression &sexpr);
	unique_ptr<ParsedExpression> TransformNested(const skysubstrait::Expression &sexpr);

	static void VerifyCorrectExtractSubfield(const string &subfield);
	static string RemapFunctionName(const string &function_name);
	static string RemoveExtension(const string &function_name);
	static LogicalType SubstraitToDuckType(const skysubstrait::Type &s_type);
	//! Looks up for aggregation function in functions_map
	string FindFunction(uint64_t id);

	//! Transform Substrait Sort Order to DuckDB Order
	OrderByNode TransformOrder(const skysubstrait::SortField &sordf);
	//! DuckDB Connection
	Connection &con;
	//! Substrait Plan
	skysubstrait::Plan plan;
	//! Variable used to register functions
	unordered_map<uint64_t, string> functions_map;
	//! Remapped functions with differing names to the correct DuckDB functions
	//! names
	static const unordered_map<std::string, std::string> function_names_remap;
	static const case_insensitive_set_t valid_extract_subfields;
	vector<ParsedExpression *> struct_expressions;
};
} // namespace duckdb
