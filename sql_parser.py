import sqlparse

def get_table_names(token_list):
    """Extracts table names from a list of tokens."""
    tables = []
    for token in token_list:
        if isinstance(token, sqlparse.sql.Identifier):
            tables.append(token.get_real_name())
        elif isinstance(token, sqlparse.sql.IdentifierList):
            for identifier in token.get_identifiers():
                if isinstance(identifier, sqlparse.sql.Identifier): # Check if it's an Identifier
                    tables.append(identifier.get_real_name())
    return list(set(tables)) # Remove duplicates

def get_column_names(token_list):
    """Extracts column names from a list of tokens."""
    columns = []
    # Flag to identify if we are in a WHERE clause, to avoid extracting columns from conditions
    in_where_clause = False
    # Flag to identify if we are in a function call, to avoid extracting function names as columns
    in_function = False

    for token in token_list:
        if token.is_keyword and token.value.upper() == 'WHERE':
            in_where_clause = True
        if token.is_keyword and token.value.upper() in ('SELECT', 'INSERT', 'UPDATE'): # Reset for new clauses
            in_where_clause = False

        if isinstance(token, sqlparse.sql.Function):
            in_function = True # Entering a function call

        if not in_where_clause and not in_function:
            if isinstance(token, sqlparse.sql.Identifier):
                # Check if the identifier is part of a function or an alias
                parent = token.parent
                is_part_of_function = isinstance(parent, sqlparse.sql.Function)
                is_alias = False
                if parent:
                    # Check if the token is an alias (e.g., "col AS alias_name")
                    # This requires looking at siblings or more complex logic,
                    # for simplicity, we'll assume simple identifiers are columns for now.
                    idx = -1
                    if parent.tokens:
                        try:
                            idx = parent.tokens.index(token)
                        except ValueError:
                            pass # token not in parent.tokens, should not happen

                    if idx > 0 and parent.tokens[idx-1].is_keyword and parent.tokens[idx-1].value.upper() == 'AS':
                         is_alias = True
                    # Further checks can be added for aliased tables like "table_alias.column"

                if not is_part_of_function and not is_alias:
                    columns.append(token.get_real_name())

            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    if isinstance(identifier, sqlparse.sql.Identifier):
                        # Similar checks for functions/aliases within IdentifierList
                        parent = identifier.parent
                        is_part_of_function_in_list = isinstance(parent, sqlparse.sql.Function) # Unlikely here but good check
                        if not is_part_of_function_in_list:
                             columns.append(identifier.get_real_name())

        if in_function and token.ttype is sqlparse.tokens.Punctuation and token.value == ')':
            in_function = False # Exiting a function call


    return list(set(columns)) # Remove duplicates


def parse_sql(sql_query):
    """
    Parses an SQL query and extracts basic information like statement type,
    table names, and column names.
    """
    parsed = sqlparse.parse(sql_query)
    if not parsed:
        print("Could not parse SQL query.")
        return

    stmt = parsed[0] # Assuming the first statement is the one we want to analyze

    print(f"Original Query: {sql_query.strip()}")
    print(f"Statement Type: {stmt.get_type()}")

    tables = []
    columns = []

    # Iterate through tokens to find tables and columns
    # This is a simplified approach. For complex queries with subqueries,
    # CTEs, joins, etc., a more sophisticated traversal of the token tree is needed.

    # --- Logic to extract tables ---
    from_seen = False
    table_identifiers = []

    # For INSERT statements, the table is usually after INTO
    if stmt.get_type() == 'INSERT':
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'INTO':
                # The next non-whitespace token should be the table identifier or list
                idx = stmt.tokens.index(token)
                for t in stmt.tokens[idx+1:]:
                    if t.is_whitespace:
                        continue
                    if isinstance(t, sqlparse.sql.Identifier):
                        tables.append(t.get_real_name())
                        break
                    elif isinstance(t, sqlparse.sql.IdentifierList):
                         for identifier in t.get_identifiers():
                            tables.append(identifier.get_real_name())
                         break
                    break
                break
    # For UPDATE statements, the table is usually the first identifier
    elif stmt.get_type() == 'UPDATE':
        for token in stmt.tokens:
            if isinstance(token, sqlparse.sql.Identifier):
                tables.append(token.get_real_name())
                break # Assuming the first identifier is the table
    # For DELETE statements, table is after FROM
    elif stmt.get_type() == 'DELETE':
        from_keyword_seen = False
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'FROM':
                from_keyword_seen = True
                continue
            if from_keyword_seen and isinstance(token, sqlparse.sql.Identifier):
                tables.append(token.get_real_name())
                break # Found the table
            elif from_keyword_seen and isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    tables.append(identifier.get_real_name())
                break
    # For SELECT statements, tables are after FROM and JOIN
    elif stmt.get_type() == 'SELECT':
        _tables = []
        from_or_join_seen = False
        # Iterate through all tokens to find tables
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() in ('FROM', 'JOIN'):
                from_or_join_seen = True
                continue

            # Reset if we encounter other keywords that usually follow table names in these clauses
            if from_or_join_seen and token.is_keyword and token.value.upper() not in ('AS', 'ON'):
                 # Keywords like WHERE, GROUP BY, ORDER BY, LIMIT, etc., or another JOIN/FROM
                if token.value.upper() not in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS'): # JOIN types
                    from_or_join_seen = False # End of current table list

            if from_or_join_seen:
                if isinstance(token, sqlparse.sql.Identifier):
                    # Check if this identifier is an alias for a preceding identifier (table)
                    # This is a simplified check. A robust alias detection is harder.
                    is_table_alias = False
                    idx = -1
                    try:
                        idx = stmt.tokens.index(token)
                    except ValueError: pass

                    if idx > 0:
                        prev_significant_token = None
                        for i in range(idx -1, -1, -1):
                            if not stmt.tokens[i].is_whitespace:
                                prev_significant_token = stmt.tokens[i]
                                break
                        if isinstance(prev_significant_token, sqlparse.sql.Identifier):
                            is_table_alias = True
                            # if prev_significant_token.get_real_name() in _tables: # Check if previous was already added as table
                            #    is_table_alias = True


                    if not is_table_alias:
                         _tables.append(token.get_real_name())
                elif isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        if isinstance(identifier, sqlparse.sql.Identifier):
                            _tables.append(identifier.get_real_name())

        tables.extend(list(set(_tables)))

    # For INSERT ... SELECT ...
    if stmt.get_type() == 'INSERT':
        is_insert_select = False
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'SELECT':
                is_insert_select = True
                break
        if is_insert_select:
            # This is an INSERT ... SELECT statement. We need to find tables in the SELECT part.
            select_stmt = None
            for token in stmt.tokens:
                if isinstance(token, sqlparse.sql.Statement) and token.get_type() == 'SELECT': # Should not happen, parse gives one statement
                    select_stmt = token
                    break
                # Heuristic: find the SELECT token and parse tokens after it as if it's a SELECT query
                # This is tricky because sqlparse might not nest a full Statement object here.
                # We'll rely on the general table finder to catch tables from the SELECT part.
                # The get_table_names helper should find all identifiers that could be tables.

            # Re-run a simplified table extraction for the SELECT part of INSERT...SELECT
            # This is a bit of a hack. A full recursive descent parser would be better.
            select_part_tokens = []
            select_keyword_found = False
            for token in stmt.tokens:
                if token.is_keyword and token.value.upper() == 'SELECT':
                    select_keyword_found = True
                if select_keyword_found:
                    select_part_tokens.append(token)

            if select_part_tokens:
                # Crude way to get tables from the SELECT part of an INSERT...SELECT
                from_or_join_seen_in_select = False
                for token in select_part_tokens:
                    if token.is_keyword and token.value.upper() in ('FROM', 'JOIN'):
                        from_or_join_seen_in_select = True
                        continue
                    if from_or_join_seen_in_select and token.is_keyword and token.value.upper() not in ('AS', 'ON'):
                        from_or_join_seen_in_select = False

                    if from_or_join_seen_in_select:
                        if isinstance(token, sqlparse.sql.Identifier):
                            tables.append(token.get_real_name())
                        elif isinstance(token, sqlparse.sql.IdentifierList):
                            for identifier_in_list in token.get_identifiers():
                                if isinstance(identifier_in_list, sqlparse.sql.Identifier):
                                    tables.append(identifier_in_list.get_real_name())
    # tables = list(set(tables)) # Defer deduplication to the end


    # --- Logic to extract columns ---
    # _columns = [] # Columns from statement-specific logic -> use main 'columns' list directly or a single _columns
    # Let's try appending directly to 'columns' from specific logic, then dedupe at end.
    # For clarity, will stick to _columns for specific handlers, then one merge and dedupe.
    _columns = []
    if stmt.get_type() == 'SELECT':
        select_seen = False
        from_seen_for_columns = False
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'SELECT':
                select_seen = True
                continue
            if token.is_keyword and token.value.upper() == 'FROM':
                from_seen_for_columns = True

            if select_seen and not from_seen_for_columns: # Only look for columns between SELECT and FROM
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier_in_list in token.get_identifiers():
                        if isinstance(identifier_in_list, sqlparse.sql.Function):
                            _columns.append(str(identifier_in_list))
                        elif isinstance(identifier_in_list, sqlparse.sql.Identifier):
                            if identifier_in_list.get_alias():
                                col_name = str(identifier_in_list.tokens[0])
                            else:
                                col_name = str(identifier_in_list)
                            _columns.append(col_name.replace('"', ''))
                        else:
                            _columns.append(str(identifier_in_list).replace('"', ''))
                elif isinstance(token, sqlparse.sql.Identifier):
                    if token.get_alias():
                        col_name = str(token.tokens[0])
                    else:
                        col_name = str(token)
                    _columns.append(col_name.replace('"', ''))
                elif isinstance(token, sqlparse.sql.Function):
                     _columns.append(str(token).replace('"', '')) # Functions might have quoted args
                elif token.ttype is sqlparse.tokens.Wildcard:
                    _columns.append(str(token))


    elif stmt.get_type() == 'INSERT':
        paren_depth = 0
        columns_identified = False
        # Check for INSERT INTO table (col1, col2)
        # The first Punctuation '(' after table name and before VALUES
        # is likely to contain column names.

        # Find table name first to know where to look for column list
        insert_into_seen = False
        table_token_passed = False
        values_keyword_seen = False

        for token_idx, token in enumerate(stmt.tokens):
            if token.is_keyword and token.value.upper() == 'INTO':
                insert_into_seen = True
                continue
            if insert_into_seen and isinstance(token, sqlparse.sql.Identifier) and not table_token_passed:
                table_token_passed = True # This is the table name
                continue
            if insert_into_seen and isinstance(token, sqlparse.sql.IdentifierList) and not table_token_passed:
                table_token_passed = True # These are the table names (e.g. schema.table)
                continue

            if token.is_keyword and token.value.upper() == 'VALUES':
                values_keyword_seen = True

            # Columns are usually in parentheses between table name and VALUES
            if table_token_passed and not values_keyword_seen:
                if token.ttype is sqlparse.tokens.Punctuation and token.value == '(':
                    # Look for IdentifierList or Identifiers within these parentheses
                    # This requires looking ahead or managing state carefully
                    next_token_idx = token_idx + 1
                    if next_token_idx < len(stmt.tokens):
                        next_token = stmt.tokens[next_token_idx]
                        if isinstance(next_token, sqlparse.sql.IdentifierList):
                            for identifier in next_token.get_identifiers():
                                if isinstance(identifier, sqlparse.sql.Identifier):
                                    _columns.append(identifier.get_real_name())
                            columns_identified = True
                            break # Found column list for INSERT
                        elif isinstance(next_token, sqlparse.sql.Identifier): # Single column in parens
                             _columns.append(next_token.get_real_name())
                             # check if next is comma or closing paren
                             further_token_idx = next_token_idx + 1
                             if further_token_idx < len(stmt.tokens) and \
                                stmt.tokens[further_token_idx].ttype is sqlparse.tokens.Punctuation and \
                                stmt.tokens[further_token_idx].value == ')':
                                columns_identified = True
                                break


        # If INSERT ... SELECT, columns might be from the SELECT part
        is_insert_select = any(t.is_keyword and t.value.upper() == 'SELECT' for t in stmt.tokens)
        if is_insert_select and not columns_identified: # If columns for INSERT not explicitly listed, get from SELECT
            select_seen = False
            from_seen_for_columns = False
            for token in stmt.tokens: # Iterate again for the SELECT part of INSERT...SELECT
                if token.is_keyword and token.value.upper() == 'SELECT':
                    select_seen = True
                    continue
                if token.is_keyword and token.value.upper() == 'FROM':
                    from_seen_for_columns = True # Stop column collection for this SELECT

                if select_seen and not from_seen_for_columns:
                    if isinstance(token, sqlparse.sql.IdentifierList):
                        for identifier_in_list in token.get_identifiers():
                            if isinstance(identifier_in_list, sqlparse.sql.Function):
                                _columns.append(str(identifier_in_list))
                            elif isinstance(identifier_in_list, sqlparse.sql.Identifier):
                                if identifier_in_list.get_alias():
                                    col_name = str(identifier_in_list.tokens[0])
                                else:
                                    col_name = str(identifier_in_list)
                                _columns.append(col_name.replace('"', ''))
                            else:
                                _columns.append(str(identifier_in_list).replace('"', ''))
                    elif isinstance(token, sqlparse.sql.Identifier):
                        if token.get_alias():
                            col_name = str(token.tokens[0])
                        else:
                            col_name = str(token)
                        _columns.append(col_name.replace('"', ''))
                    elif isinstance(token, sqlparse.sql.Function):
                        _columns.append(str(token).replace('"', ''))
                    elif token.ttype is sqlparse.tokens.Wildcard:
                         _columns.append(str(token))


    elif stmt.get_type() == 'UPDATE':
        set_seen = False
        for token in stmt.tokens:
            if token.is_keyword and token.value.upper() == 'SET':
                set_seen = True
                continue
            if set_seen and token.is_keyword and token.value.upper() == 'WHERE':
                # Stop column collection before WHERE clause
                break

            if set_seen:
                if isinstance(token, sqlparse.sql.Comparison):
                    if isinstance(token.left, sqlparse.sql.Identifier):
                        _columns.append(token.left.get_real_name())
                elif isinstance(token, sqlparse.sql.Identifier):
                    # Check if this identifier is followed by an equals sign,
                    # it could be part of a list like: SET col1 = val1, col2 = val2
                    # This requires looking ahead.
                    current_token_index = -1
                    try:
                        current_token_index = stmt.tokens.index(token)
                    except ValueError: pass

                    if current_token_index != -1 and current_token_index + 1 < len(stmt.tokens):
                        next_token = stmt.tokens[current_token_index+1]
                        # Skip whitespace to find the actual next token
                        i = current_token_index + 1
                        while i < len(stmt.tokens) and stmt.tokens[i].is_whitespace:
                            i += 1
                        if i < len(stmt.tokens) and stmt.tokens[i].ttype == sqlparse.tokens.Operator and stmt.tokens[i].value == '=':
                             _columns.append(token.get_real_name())

    # Consolidate all found tables and columns
    tables = sorted(list(set(tables))) # Deduplicate and sort tables

    # Merge _columns (from specific logic) into main columns list, then deduplicate and sort
    columns.extend(_columns) # _columns has been populated by specific handlers
    columns = sorted(list(set(columns))) # Deduplicate and sort columns

    # Final filtering of columns
    sql_keywords_upper = {
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
        'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON', 'GROUP', 'BY', 'ORDER', 'HAVING',
        'LIMIT', 'AS', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT'
    }
    if columns:
        final_tables_set = set(tables) # Use the deduplicated and sorted tables list for filtering
        columns = [col for col in columns if col not in final_tables_set and col.upper() not in sql_keywords_upper]
        # The columns list is already sorted. The filter might not preserve order perfectly
        # if items are removed, but for typical cases, it should be mostly fine.
        # Re-sorting if strict order is needed after filtering:
        columns = sorted(list(set(columns)))


    print(f"Tables: {tables if tables else 'N/A'}")
    print(f"Columns: {columns if columns else 'N/A'}")
    print("-" * 30)


if __name__ == "__main__":
    print("--- SQL Parser Examples ---")

    parse_sql("SELECT id, name, email FROM users WHERE age > 30;")
    parse_sql("INSERT INTO products (name, price, category_id) VALUES ('Laptop', 1200.00, 1);")
    parse_sql("UPDATE customers SET address = '123 Main St', city = 'Anytown' WHERE id = 101;")
    parse_sql("DELETE FROM orders WHERE order_date < '2023-01-01';")
    parse_sql("SELECT c.name, o.order_date FROM customers c JOIN orders o ON c.id = o.customer_id;")
    parse_sql("SELECT COUNT(*) AS total_users FROM users;")
    parse_sql("INSERT INTO logs VALUES (1, 'User logged in', NOW());") # Implicit columns
    parse_sql("CREATE TABLE new_users (id INT, name VARCHAR(100));") # DDL example (parser will identify type)
    parse_sql("SELECT DISTINCT department FROM employees;")
    parse_sql("UPDATE user_settings SET theme = 'dark';") # No WHERE clause
    parse_sql("SELECT column1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.fk_id WHERE t1.status = 'active';")

    # More complex examples
    parse_sql("""
        SELECT u.id, u.name, p.product_name
        FROM users u
        INNER JOIN profiles pf ON u.id = pf.user_id
        LEFT JOIN orders o ON u.id = o.user_id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        WHERE u.status = 'active' AND pf.is_verified = TRUE
        GROUP BY u.id, u.name, p.product_name
        HAVING COUNT(o.id) > 0
        ORDER BY u.name;
    """)
    parse_sql("INSERT INTO employees (id, name, department_id) SELECT user_id, full_name, dept_id FROM staging_users WHERE is_employee = TRUE;")
    parse_sql("DELETE FROM shopping_cart_items;") # Delete all rows

    print("--- Testing with potentially tricky names ---")
    parse_sql("SELECT \"user\".id, \"user\".\"full name\" FROM \"user\" WHERE \"user\".status = 'active';")
    parse_sql("UPDATE \"order details\" SET quantity = 5 WHERE product_id = 10;")

    print("\nNote: Column and table extraction can be complex for very nested queries, subqueries, or CTEs." +
          " This script provides basic extraction.")
