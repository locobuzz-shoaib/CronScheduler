import json
import sys

print(f"in config file before reading the file")

try:
    with open("appsettings.json", "r") as file:
        configuration = json.load(file)
except (ValueError, json.JSONDecodeError):
    print(f"Error while decoding the json ")
    sys.exit(0)
except FileNotFoundError:
    print(f"The file appsettings.json does not exist.")
    sys.exit(0)
except PermissionError:
    print(f"Permission denied. Unable to open the file appsettings.json")
    sys.exit(0)

try:
    ENVIRON = configuration.get("environ", "")
    SQL_USERNAME = configuration.get("sql_user_name")
    SQL_PASSWORD = configuration.get("sql_pass_word")
    SQL_SERVER = configuration.get("sql_server_ip")
    GCHAT_WEBHOOK = configuration.get("hook")
    GCHAT_WEBHOOK_ERROR = configuration.get("error_log")
    MAILGUN = configuration.get("main_gun_key")
    MSSQL_AUTH = {
        "rw": {
            "username": SQL_USERNAME,
            "password": SQL_PASSWORD,
            "db_name": "Spatialrss",
            "IP": SQL_SERVER,
            "port": 1401
        }}

    LOG_ENABLED = configuration.get("log_enabled", "production")
    log_env = LOG_ENABLED.split(",")
    LOG_ENABLED = [item.lower() for item in log_env]

except KeyError as e:
    print(f"Failed to initialize the project key: {e} is missing or not present")
    sys.exit(0)
except Exception as e:
    print(f"Failed to initialize the project due to: {e} ")
    sys.exit(0)
