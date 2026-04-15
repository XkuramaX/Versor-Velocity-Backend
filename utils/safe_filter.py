import polars as pl
from typing import Any, List

class SafeFilterBuilder:
    """Build safe Polars filter expressions without using eval()"""
    
    NUMERIC_OPS = {
        'gt': lambda l, r: l > r,
        'gte': lambda l, r: l >= r,
        'lt': lambda l, r: l < r,
        'lte': lambda l, r: l <= r,
        'eq': lambda l, r: l == r,
        'ne': lambda l, r: l != r,
        'between': lambda l, r: (l >= r[0]) & (l <= r[1]),
        'in': lambda l, r: l.is_in(r)
    }
    
    STRING_OPS = {
        'eq': lambda l, r: l == r,
        'ne': lambda l, r: l != r,
        'contains': lambda l, r: l.str.contains(r if isinstance(r, str) else r),
        'starts_with': lambda l, r: l.str.starts_with(r if isinstance(r, str) else r),
        'ends_with': lambda l, r: l.str.ends_with(r if isinstance(r, str) else r),
        'in': lambda l, r: l.is_in(r)
    }
    
    @staticmethod
    def _resolve_value(value: Any, value_type: str = None, available_columns: list = None):
        """Resolve a value — if it matches a column name, return pl.col(); otherwise literal."""
        if available_columns and isinstance(value, str) and value in available_columns:
            return pl.col(value)
        if value_type == 'numeric':
            try:
                return float(value)
            except (ValueError, TypeError):
                return value
        return value

    @staticmethod
    def build_filter(column: str, operation: str, value: Any, column_type: str = 'numeric',
                     compare_column: str = None, available_columns: list = None):
        """Build a safe filter expression.
        If compare_column is set, compare against that column instead of a literal value."""
        ops = SafeFilterBuilder.NUMERIC_OPS if column_type == 'numeric' else SafeFilterBuilder.STRING_OPS
        
        if operation not in ops:
            raise ValueError(f"Invalid operation '{operation}' for {column_type} type")
        
        left = pl.col(column)
        
        if compare_column:
            right = pl.col(compare_column)
        else:
            right = value
        
        return ops[operation](left, right)
    
    @staticmethod
    def build_multi_filter(filters: List[dict], logic: str = 'and'):
        """Build multiple filter expressions combined with AND/OR"""
        if not filters:
            raise ValueError("At least one filter required")
        
        expressions = []
        for f in filters:
            expressions.append(
                SafeFilterBuilder.build_filter(
                    f['column'], 
                    f['operation'], 
                    f['value'], 
                    f.get('column_type', 'numeric'),
                    compare_column=f.get('compare_column'),
                )
            )
        
        if logic == 'and':
            result = expressions[0]
            for expr in expressions[1:]:
                result = result & expr
            return result
        else:
            result = expressions[0]
            for expr in expressions[1:]:
                result = result | expr
            return result
