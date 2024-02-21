import datetime
import time


class LogFileManager(object):
    def __init__(self, engine):
        self.log_files = []
        filename = f"error/{engine}.log"
        if engine == "postgres":
            formatted_time = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
            filename = f"error/postgresql.log.{formatted_time}"
        self.log_files.append(DBLogFile(filename))

    @property
    def files(self):
        return self.log_files


class DBLogFile(object):
    def __init__(self, name):
        self.log_file_name = name
        self.last_written = int(time.time())
        self.size = 123

    @property
    def resource_id(self):
        return f"{self.log_file_name}-{self.last_written}-{self.size}"


class LogBackend:
    def __init__(self):
        self.events = []

    def describe_db_log_files(self, db_instance_identifier):
        database = self.get_db_instance(db_instance_identifier)
        return database.log_file_manager.files