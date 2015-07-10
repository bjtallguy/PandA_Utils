__author__ = 'BJ'
"""A utility script for sorting an IIS log file by session id and preparing various reports."""

import re
import sqlite3
from datetime import datetime


# Global configuration values
DB = 'dotnet.db'
DOTNET_LOG = r"//sbsfiles0/IQA_SQA_Central/IQA - Automation/tmp/emortgages/dotnetlogs.csv"
OUT_FILE = "report.txt"


# Report definition elements
REPORT_FIELDS = ['sessionid', 'time', 'level', 'message']
REPORT_HEADER = """This report contains {session_count} sessions."""
SECTION_HEADER = "SessionID:{session_id}  Rows:{row_count}  Max Wait:{max_wait}  Total Time:{session_time}"
SECTION_LINE = """[{level}] {time} : {message}"""
DASH_LENGTH = 120


class ReportSectionFunctions:
    """Functions that return values for a given set of rows.
    rows are lists of dictionaries, each representing a record from the database.
    """

    def __init__(self, rows, order_key='time'):
        self.order_key = order_key
        self.rows = list(rows)
        self.process_rows()
        self.header_values = self.get_header_dict()

    def process_timestamp(self, time):
        """Some timestamps are coming back without partial seconds :("""
        try:
            proc_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            proc_time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        return proc_time

    def process_rows(self):
        """Pre-processing for database rows, before the following functions are used."""
        self.rows.sort(key=lambda k: k[self.order_key])

    def row_count(self):
        return len(self.rows)

    def max_wait(self):
        """Return the maximum wait between ordered time fields."""
        deltas = []
        prev = self.process_timestamp(self.rows[0]['time'])
        for r in self.rows:
            current = self.process_timestamp(r['time'])
            deltas.append(current - prev)
            prev = current
        return max(deltas)

    def session_time(self):
        """
        Calculates the time between sessin start and finish.
        float:return: time in msec
        """
        start = self.process_timestamp(self.rows[0]['time'])
        end = self.process_timestamp(self.rows[-1]['time'])
        return end - start

    def get_header_dict(self):
        """Return a dictionary of values required for each section."""
        return {'row_count': self.row_count(),
                'session_id': self.rows[0]['sessionid'],
                'session_time': self.session_time(),
                'max_wait': self.max_wait()}


def extract_time_stamp(line):
    time_string = re.findall(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}", line)
    if not time_string:
        raise IndexError("Timestamp not found!")
    else:
        time_string = time_string[0]
    (year, month, day, hour, minute, second, microsecond) = (time_string[0:4], time_string[5:7], time_string[8:10],
                                                             time_string[11:13], time_string[14:16], time_string[17:19],
                                                             time_string[20:23])
    return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), int(microsecond)*1000)


def extract_log_level(line):
    line_level = None
    log4j_levels_by_severity = ['OFF', 'DEBUG', 'TRACE', 'INFO', 'WARN', 'ERROR', 'FATAL']
    for level in log4j_levels_by_severity:
        if re.findall(r"\[\d{1,3}\] %s " % level, line):
            line_level = level
    if line_level:
        return line_level
    else:
        raise IndexError("Log Level not found in line!")


def extract_session_id(line):
    try:
        return re.findall(r"(?<=SessionID:)\w{24}", line)[0]
    except IndexError:
        raise IndexError("Session ID not found!")


def extract_message(line):
    try:
        return line.split("] - [")[1]
    except IndexError:
        raise IndexError("Failed to split line on hyphen. No message extracted!")


def process_log_line(line):
    timestamp = extract_time_stamp(line)
    log_level = extract_log_level(line)
    session_id = extract_session_id(line)
    message = extract_message(line).replace("SessionID:%s] " % session_id, "")
    return {'time': timestamp, 'level': log_level, 'sessionid': session_id, 'message': message}


def get_connection(database=None):
    connection = sqlite3.connect(database or DB)
    connection.row_factory = sqlite3.Row
    return connection


def populate_db(database, log_file):
    with open(log_file, newline='') as log_lines:
        conn = get_connection(database)
        cursor = conn.cursor()
        for line in log_lines:
            try:
                log = process_log_line(line)
            except IndexError:
                pass
            else:
                cursor.execute("INSERT INTO log (time, level, sessionid, message) VALUES (?, ?, ?, ?)",
                               (log['time'], log['level'], log['sessionid'], log['message']))
        cursor.close()
        conn.commit()


def create_db(db_file):
    conn = get_connection(db_file)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS log")
    cursor.execute("CREATE TABLE log (time timestamp, level text, sessionid text, message text)")
    cursor.close()
    conn.commit()


def generate_report(database, report_file):
    c = get_connection(database).cursor()
    f = open(report_file, mode='w')

    session_ids = c.execute("SELECT DISTINCT sessionid FROM log ORDER BY time").fetchall()

    print("\n\n This report includes %s sessions \n" % (len(session_ids)) + ("-" * DASH_LENGTH), file=f)

    for session_id in [r['sessionid'] for r in session_ids]:

        messages = c.execute("SELECT sessionid, message, level, time as \"time [timestamp]\" "
                             "FROM log WHERE sessionid=? ORDER BY time", (session_id, )).fetchall()

        print("\n" + ("-" * DASH_LENGTH), file=f)
        print(SECTION_HEADER.format_map(ReportSectionFunctions(messages).header_values), file=f)
        print(("-" * DASH_LENGTH) + "\n", file=f)

        for msg in messages:
            print(SECTION_LINE.format_map(msg).strip(), file=f)

    c.close()
    f.close()


if __name__ == '__main__':
    # create_db(DB)
    # populate_db(DB, DOTNET_LOG)
    generate_report(DB, OUT_FILE)
