import json
from typing import Dict, List, Any
from controllers.DataframeController import DataframeController


class WorkflowManager:

    def __init__(self, controller: DataframeController):
        self.controller = controller
        self.nodes: Dict[str, Dict] = {}

    def create_node(self, node_type: str, params: Dict[str, Any], parent_id: str = None):
        """Standard Creation"""
        try:
            new_node_id = self._execute_controller_method(node_type, params, parent_id)

            if isinstance(new_node_id, dict) and new_node_id.get("status") == "error":
                raise ValueError(f"{new_node_id['type']}: {new_node_id['msg']}")

            self.nodes[new_node_id] = {
                "type": node_type,
                "params": params,
                "parent": parent_id,
                "children": []
            }
            if parent_id and parent_id in self.nodes:
                self.nodes[parent_id]["children"].append(new_node_id)
            return new_node_id
        except Exception as e:
            raise ValueError(str(e))

    def edit_node(self, node_id: str, new_params: Dict[str, Any]):
        """Update node parameters and re-propagate logic to child nodes."""
        if node_id not in self.nodes:
            return {"error": "Node not found"}

        node = self.nodes[node_id]
        node["params"] = new_params
        self._execute_controller_method(node["type"], new_params, node["parent"])

        for child_id in node["children"]:
            self.edit_node(child_id, self.nodes[child_id]["params"])
        return {"status": "success", "node_id": node_id}

    def delete_node(self, node_id: str):
        """Remove a node and clean up the registry."""
        if node_id not in self.nodes:
            return {"error": "Node not found"}

        try:
            for child_id in list(self.nodes[node_id]["children"]):
                self.delete_node(child_id)

            if node_id in self.controller.registry:
                del self.controller.registry[node_id]

            parent_id = self.nodes[node_id]["parent"]
            if parent_id and parent_id in self.nodes:
                self.nodes[parent_id]["children"].remove(node_id)

            del self.nodes[node_id]
            return {"status": "deleted"}
        except (KeyError, ValueError) as e:
            return {"error": f"Failed to delete node: {str(e)}"}

    # ── Allowed method whitelist ──────────────────────────────────────────────
    ALLOWED_METHODS = {
        'read_csv', 'read_excel',
        # Transform
        'safe_filter_data', 'select_columns', 'drop_columns', 'sort_data',
        'rename_columns', 'reorder_columns',
        # String
        'str_to_upper', 'str_to_lower', 'str_to_title',
        'str_left', 'str_right', 'str_mid',
        'concat_columns', 'concat_with_literal', 'clean_string_column',
        # Math
        'horizontal_sum', 'horizontal_average',
        'apply_custom_expression', 'multi_column_multiply',
        # Vector
        'vector_dot_product', 'vector_linear_multiply', 'vector_cross_product',
        # Cleaning
        'drop_na', 'drop_nulls', 'drop_duplicates', 'fill_missing',
        # Types / Date
        'cast_column', 'extract_date_parts',
        # Joins
        'join_nodes', 'union_nodes', 'append_vertical',
        # Analytics
        'filter_outliers_iqr', 'group_by_agg', 'col_stats_advanced',
        'pivot_table', 'moving_average', 'conditional_column',
        # Matrix
        'matrix_transpose',
        # ML
        'linear_regression_node', 'logistic_regression_prediction',
        'correlation_matrix_1d',
        # New nodes
        'add_literal_column', 'range_bucket', 'date_offset', 'crosstab', 'cumulative_product',
        # Enhanced regression + stats + charts
        'ols_regression', 't_test', 'f_test', 'chi_square_test', 'dw_test', 'anova_test', 'chart_node',
        # Roll-rate analysis
        'monthly_snapshot', 'transition_matrix', 'period_average_matrix', 'chain_probability',
    }

    # ── Param whitelist (prevents mass-assignment) ────────────────────────────
    PARAM_WHITELIST: Dict[str, set] = {
        'read_csv':                       {'file_path'},
        'read_excel':                     {'file_path', 'sheet_name'},
        # Transform
        'safe_filter_data':               {'filters', 'logic'},
        'select_columns':                 {'columns'},
        'drop_columns':                   {'columns'},
        'sort_data':                      {'by', 'descending'},
        'rename_columns':                 {'mapping'},
        'reorder_columns':                {'ordered_cols'},
        # String
        'str_to_upper':                   {'column', 'new_col'},
        'str_to_lower':                   {'column', 'new_col'},
        'str_to_title':                   {'column', 'new_col'},
        'str_left':                       {'column', 'n_chars', 'new_col'},
        'str_right':                      {'column', 'n_chars', 'new_col'},
        'str_mid':                        {'column', 'start', 'length', 'new_col'},
        'concat_columns':                 {'columns', 'separator', 'new_col'},
        'concat_with_literal':            {'column', 'prefix', 'suffix', 'new_col'},
        'clean_string_column':            {'column'},
        # Math
        'horizontal_sum':                 {'columns', 'new_col'},
        'horizontal_average':             {'columns', 'new_col'},
        'apply_custom_expression':        {'left_cols', 'op', 'right_val', 'new_suffix'},
        'multi_column_multiply':          {'columns', 'factor', 'suffix'},
        # Vector
        'vector_dot_product':             {'vec_a', 'vec_b', 'new_col'},
        'vector_linear_multiply':         {'vec_a', 'vec_b', 'suffix'},
        'vector_cross_product':           {'vec_a', 'vec_b', 'prefix'},
        # Cleaning
        'drop_na':                        {'subset'},
        'drop_nulls':                     set(),
        'drop_duplicates':                {'subset'},
        'fill_missing':                   {'column', 'value'},
        # Types / Date
        'cast_column':                    {'column', 'dtype'},
        'extract_date_parts':             {'column'},
        # Joins
        'join_nodes':                     {'right_id', 'on', 'how'},
        'union_nodes':                    {'node_ids'},
        'append_vertical':                {'top_node_id', 'bottom_node_id', 'how'},
        # Analytics
        'filter_outliers_iqr':            {'column'},
        'group_by_agg':                   {'group_cols', 'aggs'},
        'col_stats_advanced':             {'columns'},
        'pivot_table':                    {'values', 'index', 'on', 'agg'},
        'moving_average':                 {'column', 'window'},
        # Safe conditional — no raw expression string allowed
        'conditional_column':             {'column', 'op', 'threshold', 'then_val', 'else_val', 'new_col'},
        # Matrix
        'matrix_transpose':               set(),
        # ML
        'linear_regression_node':         {'target', 'features'},
        'logistic_regression_prediction': {'features', 'weights'},
        'correlation_matrix_1d':          {'columns'},
        # New nodes
        'add_literal_column':             {'column', 'value', 'dtype'},
        'range_bucket':                   {'column', 'bins', 'labels', 'new_col'},
        'date_offset':                    {'column', 'offset', 'unit', 'new_col'},
        'crosstab':                       {'index', 'columns', 'values', 'agg'},
        'cumulative_product':             {'column', 'new_col'},
        # Enhanced regression + stats + charts
        'ols_regression':                 {'target', 'features'},
        't_test':                         {'column_a', 'column_b', 'test_type', 'alternative', 'popmean'},
        'f_test':                         {'column_a', 'column_b'},
        'chi_square_test':                {'column_a', 'column_b'},
        'dw_test':                        {'residuals_col'},
        'anova_test':                     {'value_col', 'group_col'},
        'chart_node':                     {'chart_type', 'x_col', 'y_col', 'color_col', 'title', 'bins', 'agg'},
        # Roll-rate analysis
        'monthly_snapshot':                {'id_col', 'date_col', 'value_col', 'agg'},
        'transition_matrix':               {'id_col', 'period_col', 'bucket_col', 'bucket_order'},
        'period_average_matrix':           {'window', 'bucket_order'},
        'chain_probability':               {'bucket_order'},
    }

    def _execute_controller_method(self, node_type: str, params: Dict[str, Any], parent_id: str):
        if node_type not in self.ALLOWED_METHODS:
            raise ValueError(f"Method '{node_type}' is not allowed")

        try:
            method = getattr(self.controller, node_type)
            validated_params = self._validate_params(node_type, params)
            return method(parent_id, **validated_params) if parent_id else method(**validated_params)
        except AttributeError as e:
            raise ValueError(f"Method '{node_type}' not found in controller: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error executing {node_type}: {str(e)}")

    def _validate_params(self, node_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Strip any keys not in the whitelist to prevent mass-assignment."""
        allowed = self.PARAM_WHITELIST.get(node_type, set())
        return {k: v for k, v in params.items() if k in allowed}
