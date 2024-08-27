#include "custom_extensions/custom_extensions.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// FIXME: This cannot be the best way of getting string names of the types
string TransformTypes(const substrait::Type &type) {
	auto str = type.DebugString();
	string str_type;
	for (auto &c : str) {
		if (c == ' ') {
			return str_type;
		}
		str_type += c;
	}
	return str_type;
}

vector<string> GetAllTypes() {
	return {
     {"bool"}
    ,{"i8"}                 , {"i16"}         , {"i32"}         , {"i64"}
    ,{"fp32"}               , {"fp64"}
    ,{"string"}             , {"binary"}
    ,{"timestamp"}          , {"date"}        , {"time"}
    ,{"interval_year"}      , {"interval_day"}, {"timestamp_tz"}
    ,{"uuid"}
    ,{"varchar"}            , {"fixed_binary"}, {"decimal"}
    ,{"precision_timestamp"}, {"precision_timestamp_tz"}
  };
}

bool IsAnyParamType(const string& param_type) {
  return param_type == "any1" || param_type == "unknown" || param_type == "any";
}

bool IsManyArgFn(vector<string>& param_types) {
  auto& first_ptype = param_types[0];
  if (first_ptype.empty() or first_ptype.back() != '?') { return false; }

  // All parameter types of a many_argument function are: (1) equal and (2) end with '?'
  for (std::string::size_type type_ndx = 1; type_ndx < param_types.size(); ++type_ndx) {
    auto& param_type = param_types[type_ndx];

    if (param_type.empty() or param_type.back() != '?' or first_ptype != param_type) {
      return false;
    }
  }

  return true;
}

// Recurse over the whole shebang

//! Given a matrix of parameter types, register all acceptable substrait function signatures.
// The matrix's first dimension is for function parameters, the second dimension is for
// acceptable types of a particular parameter
void SubstraitCustomFunctions::InsertAllFunctions( const vector<vector<string>>& fn_param_types
                                                  ,vector<idx_t>&                ptype_indices
                                                  ,int                           param_ndx
                                                  ,string&                       name
                                                  ,string&                       file_path) {
  // With the given parameter types (each type determined by `ptype_indices`)
  if (param_ndx == ptype_indices.size()) {
    vector<string> types;

    // Grab the type string for each parameter
    // (use `ptype_indices` to index into `fn_param_types`)
    for (idx_t i = 0; i < ptype_indices.size(); i++) {
      auto type = fn_param_types[i][ptype_indices[i]];
      types.push_back(StringUtil::Replace(type, "boolean", "bool"));
    }

    // Then, register the function signature (and extension)
    InsertCustomFunction(name, file_path, types);

    return;
  }

  // For each acceptable type of the current parameter, recurse on the next parameter
  for (int sig_ndx = 0; sig_ndx < fn_param_types[param_ndx].size(); ++sig_ndx) {
    ptype_indices[param_ndx] = sig_ndx;
    InsertAllFunctions(fn_param_types, ptype_indices, param_ndx + 1, name, file_path);
  }

}

//! Inserts a substrait function with the given information into a registry
void SubstraitCustomFunctions::InsertCustomFunction( string         fn_name
                                                    ,string         ext_fpath
                                                    ,vector<string> param_types) {
  SubstraitCustomFunction       custom_fn     { fn_name, param_types };
  SubstraitFunctionExtensions&& custom_fn_ext { custom_fn, std::move(ext_fpath) };

  // If there were no parameters, register a variadic function signature
  if (param_types.empty()) { any_arg_functions[custom_fn] = custom_fn_ext; }

  // If there were many identical parameters suffixed with '?', register a
  // "many-argument" function signature
  else if (IsManyArgFn(param_types)) { many_arg_functions[custom_fn] = custom_fn_ext; }

  // Otherwise, register a standard function signature
  else { custom_functions[custom_fn] = custom_fn_ext; }
}

//! Using the given information for a substrait function extension, registers all
//! acceptable function signatures for the substrait function.
void SubstraitCustomFunctions::InsertFunctionExtension( string         name_p
                                                       ,vector<string> types_p
                                                       ,string         file_path) {
  // A vector of param types as specified by config
	auto cfg_param_types = std::move(types_p);

  // A vector of actual param types (after expansion)
	vector<vector<string>> fn_param_types;
	for (auto &param_type : cfg_param_types) {
    // Expand an `any` type to all possible parameter types
		if (IsAnyParamType(param_type)) { fn_param_types.emplace_back(GetAllTypes()); }

    // Push the specified param type only (single-element vector)
    else { fn_param_types.push_back({param_type}); }
	}

	// Get the number of dimensions
	idx_t count_params = fn_param_types.size();

  // A vector of integers that represents a counter of function signatures. Whenever a
  // signature is inserted into the custom function maps, `fn_sig_idx` is incremented.
  // For example, { 2, 0 }, a two parameter signature that we have inserted two of:
  // { 0, 0 } and { 1, 0 }
	vector<idx_t> fn_sig_idx(count_params, 0);

	// Call the helper function with initial depth 0
	InsertAllFunctions(fn_param_types, fn_sig_idx, 0, name_p, file_path);
}

string SubstraitCustomFunction::GetName() {
	if (arg_types.empty()) {
		return name;
	}
	string function_signature = name + ":";
	for (auto &type : arg_types) {
		function_signature += type + "_";
	}
	function_signature.pop_back();
	return function_signature;
}

string SubstraitFunctionExtensions::GetExtensionURI() const {
	if (IsNative()) {
		return "";
	}
	return "https://github.com/substrait-io/substrait/blob/main/extensions/" + extension_path;
}

bool SubstraitFunctionExtensions::IsNative() const {
	return extension_path == "native";
}

SubstraitCustomFunctions::SubstraitCustomFunctions() { Initialize(); }

vector<string> SubstraitCustomFunctions::GetTypes(const vector<substrait::Type> &types) {
	vector<string> transformed_types;
	for (auto &type : types) {
		transformed_types.emplace_back(TransformTypes(type));
	}
	return transformed_types;
}

// FIXME: We might have to do DuckDB extensions at some point
SubstraitFunctionExtensions SubstraitCustomFunctions::Get(const string &name,
                                                          const vector<substrait::Type> &types) const {
	vector<string> transformed_types;
	if (types.empty()) {
		SubstraitCustomFunction custom_function {name, {}};
		auto it = any_arg_functions.find(custom_function);
		if (it != custom_functions.end()) {
			// We found it in our substrait custom map, return that
			return it->second;
		}
		return {{name, {}}, "native"};
	}

	for (auto &type : types) {
		transformed_types.emplace_back(TransformTypes(type));
		if (transformed_types.back().empty()) {
			// If it is empty it means we did not find a yaml extension, we return the function name
			return {{name, {}}, "native"};
		}
	}
	{
		SubstraitCustomFunction custom_function {name, {transformed_types}};
		auto it = custom_functions.find(custom_function);
		if (it != custom_functions.end()) {
			// We found it in our substrait custom map, return that
			return it->second;
		}
	}

	// check if it's a many argument fit
	bool possibly_many_arg = true;
	string type = transformed_types[0];
	for (auto &t : transformed_types) {
		possibly_many_arg = possibly_many_arg && type == t;
	}
	if (possibly_many_arg) {
		type += '?';
		SubstraitCustomFunction custom_many_function {name, {{type}}};
		auto many_it = many_arg_functions.find(custom_many_function);
		if (many_it != many_arg_functions.end()) {
			return many_it->second;
		}
	}
	// TODO: check if this should also print the arg types or not
	// we did not find it, return it as a native substrait function
	return {{name, {}}, "native"};
}

} // namespace duckdb
