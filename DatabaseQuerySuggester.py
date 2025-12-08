import psycopg
from psycopg import Error
import re

class PostgreSQLManager:
    # We have 2 values to save, the connection and a cursor to manage giving commands to postgreSQL
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    #Create a persistent connection to PostgreSQL database
    def create_connection(self, host, database, user, password, port=5432):
        try:
            self.connection = psycopg.connect(
                host=host,
                dbname=database,
                user=user,
                password=password,
                port=port
            )
            self.cursor = self.connection.cursor()
            print(f"Successfully connected to database: {database}")
            return True
        except Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return False
    
    #Close the persistent connection
    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Connection closed successfully")
    
    # Suggest an aggregate version of the query for numeric columns
    # May not make sense for a lot of numeric columns, and we don't know what columns are useful, so we only really do this is the user selects a specific column.
    def suggest_aggregate_query(self, sql_command):
        sql_upper = sql_command.upper()
        
        # Only process SELECT queries
        if not sql_upper.strip().startswith('SELECT'):
            return None
        
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_command, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return None
        
        select_clause = select_match.group(1).strip()
        
        if re.search(r'\b(COUNT|SUM|AVG|MIN|MAX|GROUP BY)\b', sql_command, re.IGNORECASE):
            return None
        
        if select_clause == '*':
            return None  # Can't aggregate on *
        
        # Split by comma and clean
        columns = [col.strip() for col in select_clause.split(',')]
        
        # Execute query to retrieve column types, only suggesting if they are numeric
        try:
            self.cursor.execute(sql_command)
            if not self.cursor.description:
                return None
            
            column_info = []
            for desc in self.cursor.description:
                col_name = desc[0]
                col_type = desc[1]

                numeric_types = {23, 20, 21, 700, 701, 1700, 1114}  # common numeric type OIDs
                if col_type in numeric_types:
                    column_info.append(col_name)
            
            self.cursor.fetchall()
            
            if not column_info:
                return None
            
            # Create aggregate suggestions only for numeric columns
            agg_columns = []
            for col_name in column_info:
                agg_columns.append(f"COUNT({col_name}) as count_{col_name}, SUM({col_name}) as sum_{col_name}, AVG({col_name}) as avg_{col_name}")
            
            agg_select = ", ".join(agg_columns)
            rest_of_query = sql_command[select_match.end(1):]
            
            return f"SELECT {agg_select}{rest_of_query}"
            
        except Error:
            return None
    
    # Suggest query with flipped WHERE conditions
    def suggest_flipped_conditions(self, sql_command):
        sql_upper = sql_command.upper()
        
        # Only process queries with WHERE clause
        if 'WHERE' not in sql_upper:
            return None
        
        where_match = re.search(r'\bWHERE\b(.*?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)', sql_command, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return None
        
        where_clause = " " + where_match.group(1).strip()
        
        def flip_operator(match):
            op = match.group(0)
            flip_map = {
                '>=': '<',
                '<=': '>',
                '<>': '=',
                '!=': '=',
                '>': '<=',
                '<': '>=',
                '=': '<>'
            }
            return flip_map.get(op, op)
        
        flipped_where = re.sub(r'(>=|<=|<>|!=|>|<|=)', flip_operator, where_clause)
        
        before_where = sql_command[:where_match.start(1)]
        after_where = sql_command[where_match.end(1):]
        
        return f"{before_where}{flipped_where}{after_where}"
    
    # Print suggested alternative queries
    def print_query_suggestions(self, sql_command):
        suggestions = []
        
        agg_query = self.suggest_aggregate_query(sql_command)
        if agg_query:
            suggestions.append(("Aggregate Query", agg_query, "This query might be worth running to get an average of the data you just retrieved."))
        
        flipped_query = self.suggest_flipped_conditions(sql_command)
        if flipped_query:
            suggestions.append(("Flipped Conditions", flipped_query, "These queries could help you by giving you the data you haven't seen yet."))
        
        if suggestions:
            print("\n--- Query Suggestions ---")
            for title, query, description in suggestions:
                print(f"\n{title}:")
                print(f"  {description}")
                print(f"  {query}")
            print("-" * 60)
        if not self.connection or not self.cursor:
            print("Error: No active database connection")
            return

    # Execute SQL command and print results
    def execute_sql(self, sql_command):
        if not self.connection or not self.cursor:
            print("Error: No active database connection")
            return
        
        try:
            self.cursor.execute(sql_command)
            
            # Check if query returns results (SELECT, SHOW, etc.)
            if self.cursor.description:
                results = self.cursor.fetchall()
                column_names = [desc[0] for desc in self.cursor.description]
                
                # Print column names
                print("\n" + " | ".join(column_names))
                print("-" * (len(" | ".join(column_names))))
                
                # Print results
                for row in results:
                    print(" | ".join(str(value) for value in row))
                print(f"\n({len(results)} row(s) returned)")
            else:
                # Command has no output
                self.connection.commit()
                print(f"Command executed successfully. Rows affected: {self.cursor.rowcount}")
                
        except Error as e:
            print(f"Error executing SQL: {e}")
            self.connection.rollback()

def main():
    db = PostgreSQLManager()
    
    # Get connection details
    print("=== PostgreSQL Connection Manager ===")
    host = input("Host (default: localhost): ") or "localhost"
    database = input("Database name: ")
    user = input("Username: ")
    password = input("Password: ")
    port = input("Port (default: 5432): ") or "5432"
    
    if not db.create_connection(host, database, user, password, int(port)):
        return
    
    print("\nEnter SQL commands (type 'quit' or 'exit' to close connection and exit)")
    print("-" * 60)
    
    while True:
        try:
            command = input("\nSQL> ").strip()
            
            if command.lower() in ['quit', 'exit']:
                break
            
            if command:
                db.execute_sql(command)
                db.print_query_suggestions(command)
                
        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
            break
        except EOFError:
            break
    
    db.close_connection()
    print("Goodbye!")

if __name__ == "__main__":
    main()