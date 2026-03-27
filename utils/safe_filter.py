import polars as pl
from typing import Any, List

class SafeFilterBuilder:
    """Build safe Polars filter expressions without using eval()"""
    
    NUMERIC_OPS = {
        'gt': lambda col, val: pl.col(col) > val,
        'gte': lambda col, val: pl.col(col) >= val,
        'lt': lambda col, val: pl.col(col) < val,
        'lte': lambda col, val: pl.col(col) <= val,
        'eq': lambda col, val: pl.col(col) == val,
        'ne': lambda col, val: pl.col(col) != val,
        'between': lambda col, val: (pl.col(col) >= val[0]) & (pl.col(col) <= val[1]),
        'in': lambda col, val: pl.col(col).is_in(val)
    }
    
    STRING_OPS = {
        'eq': lambda col, val: pl.col(col) == val,
        'ne': lambda col, val: pl.col(col) != val,
        'contains': lambda col, val: pl.col(col).str.contains(val),
        'starts_with': lambda col, val: pl.col(col).str.starts_with(val),
        'ends_with': lambda col, val: pl.col(col).str.ends_with(val),
        'in': lambda col, val: pl.col(col).is_in(val)
    }
    
    @staticmethod
    def build_filter(column: str, operation: str, value: Any, column_type: str = 'numeric'):
        """Build a safe filter expression"""
        ops = SafeFilterBuilder.NUMERIC_OPS if column_type == 'numeric' else SafeFilterBuilder.STRING_OPS
        
        if operation not in ops:
            raise ValueError(f"Invalid operation '{operation}' for {column_type} type")
        
        return ops[operation](column, value)
    
    @staticmethod
    def build_multi_filter(filters: List[dict], logic: str = 'and'):
        """Build multiple filter expressions combined with AND/OR"""
        if not filters:
            raise ValueError("At least one filter required")
        
        expressions = [
            SafeFilterBuilder.build_filter(
                f['column'], 
                f['operation'], 
                f['value'], 
                f.get('column_type', 'numeric')
            )
            for f in filters
        ]
        
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
