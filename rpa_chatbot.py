import mysql.connector
import re
from datetime import datetime

# Database connection configuration
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'database': 'rpa_database'
}

# Intent-to-SQL mapping with regex patterns and response templates
INTENT_QUERIES = {
    "check_job_status": {
        "pattern": r"(?:status|check|what is).*job.*(\d+)",
        "sql": "SELECT status, start_time, end_time, duration FROM bot_executions WHERE job_id = %s",
        "response": "Job {job_id} is {status}. Started: {start_time}, Duration: {duration}."
    },
    "list_running_bots": {
        "pattern": r"(?:list|show|which).*(?:running|active).*bots",
        "sql": "SELECT bot_name, machine_name, start_time FROM bot_executions WHERE status = 'Running'",
        "response": "Running bots:\n{results}"
    },
    "top_long_running": {
        "pattern": r"(?:top|longest|most).*(?:long|running|duration).*bots.*(?:today|daily)",
        "sql": "SELECT bot_name, duration, machine_name FROM bot_executions WHERE DATE(start_time) = CURDATE() ORDER BY duration DESC LIMIT 5",
        "response": "Top 5 long-running bots today:\n{results}"
    },
    "machine_status": {
        "pattern": r"(?:status|check|what is).*machine.*(\w+)",
        "sql": "SELECT machine_name, machine_status FROM bot_executions WHERE machine_name = %s ORDER BY start_time DESC LIMIT 1",
        "response": "Machine {machine_name} is {machine_status}."
    },
    "failed_jobs": {
        "pattern": r"(?:failed|error|unsuccessful).*jobs.*(?:week|weekly)",
        "sql": "SELECT COUNT(*) as failed_jobs FROM bot_executions WHERE status = 'Failed' AND start_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)",
        "response": "{failed_jobs} jobs failed this week."
    }
}

def connect_db():
    """Connect to the SQL database."""
    try:
        return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        return f"Database connection failed: {err}"

def detect_intent(user_input):
    """Detect intent and extract parameters using regex patterns."""
    for intent, config in INTENT_QUERIES.items():
        match = re.search(config["pattern"], user_input, re.IGNORECASE)
        if match:
            return intent, match.groups()
    return None, None

def execute_query(intent, params):
    """Execute the SQL query for the given intent and parameters."""
    conn = connect_db()
    if isinstance(conn, str):
        return conn  # Return error message if connection failed
    cursor = conn.cursor(dictionary=True)
    query = INTENT_QUERIES[intent]["sql"]
    try:
        cursor.execute(query, params)
        results = cursor.fetchall()
    except mysql.connector.Error as err:
        results = f"Query error: {err}"
    finally:
        conn.close()
    return results

def format_response(intent, results, params):
    """Format the query results into a conversational response."""
    if isinstance(results, str):
        return results  # Return error message if query failed
    template = INTENT_QUERIES[intent]["response"]
    if not results:
        return "No data found for your query."
    if intent in ["list_running_bots", "top_long_running"]:
        formatted = "\n".join([f"- {row['bot_name']} on {row['machine_name']} (Started: {row['start_time']})" 
                              if intent == "list_running_bots" else 
                              f"- {row['bot_name']} on {row['machine_name']} (Duration: {row['duration']})" 
                              for row in results])
        return template.format(results=formatted)
    elif intent == "failed_jobs":
        return template.format(failed_jobs=results[0]["failed_jobs"])
    else:
        row = results[0]
        row.update({"job_id": params[0] if params else None, "machine_name": params[0] if params and intent == "machine_status" else row.get("machine_name")})
        return template.format(**row)

def chatbot(user_input):
    """Process user input and return a response."""
    intent, params = detect_intent(user_input)
    if not intent:
        return "Sorry, I don’t understand that request. Try asking about job status, running bots, or machine status."
    results = execute_query(intent, params)
    return format_response(intent, results, params)

# Example usage
if __name__ == "__main__":
    queries = [
        "What’s the status of job 1234?",
        "Which bots are currently running?",
        "Show me the top 5 longest-running bots today.",
        "What’s the status of machine X?",
        "How many jobs failed this week?"
    ]
    for query in queries:
        print(f"Query: {query}")
        print(f"Response: {chatbot(query)}\n")