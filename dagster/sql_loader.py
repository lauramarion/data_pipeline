"""
sql_loader.py
Utility to load and parse SQL queries from external files.
"""
import os
from pathlib import Path
from typing import Dict

def load_sql_queries(sql_file_path: str = "queries.sql") -> Dict[str, str]:
    """
    Load SQL queries from a file and parse them into a dictionary.
    
    Queries should be separated by comments with the format:
    -- name: query_name
    
    Args:
        sql_file_path: Path to the SQL file (relative to this module)
    
    Returns:
        Dictionary mapping query names to SQL query strings
    """
    # Get the directory where this script is located
    base_dir = Path(__file__).parent
    file_path = base_dir / sql_file_path
    
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    queries = {}
    current_name = None
    current_query = []
    
    for line in content.split('\n'):
        # Check if this line defines a query name
        if line.strip().startswith('-- name:'):
            # Save the previous query if exists
            if current_name and current_query:
                queries[current_name] = '\n'.join(current_query).strip()
            
            # Start new query
            current_name = line.split('-- name:')[1].strip()
            current_query = []
        elif current_name:
            # Skip comment lines that aren't the name
            if not line.strip().startswith('--') or line.strip() == '--':
                current_query.append(line)
    
    # Don't forget the last query
    if current_name and current_query:
        queries[current_name] = '\n'.join(current_query).strip()
    
    return queries


def get_query(query_name: str, sql_file_path: str = "queries.sql") -> str:
    """
    Get a specific query by name from the SQL file.
    
    Args:
        query_name: Name of the query to retrieve
        sql_file_path: Path to the SQL file
    
    Returns:
        SQL query string
    """
    queries = load_sql_queries(sql_file_path)
    
    if query_name not in queries:
        available = ', '.join(queries.keys())
        raise ValueError(f"Query '{query_name}' not found. Available queries: {available}")
    
    return queries[query_name]