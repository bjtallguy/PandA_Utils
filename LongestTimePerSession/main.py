__author__ = 'BJ'

import re
import csv
import sqlite3
from datetime import datetime


SAMPLE_LINE = r"2015-07-09 09:42:07,615 [11] INFO  SBS.ESA.WEBUI [(null)] - [SessionID:gunjkxtvmsr3tjvgaltoqvex]" \
              r" [Session_Start] New SessionID:gunjkxtvmsr3tjvgaltoqvex"
DB = 'dotnet.db'
DOTNET_LOG = r"//sbsfiles0/IQA_SQA_Central/IQA - Automation/tmp/emortgages/dotnetlogs.csv"


FIELD_NAMES = []


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
    cursor.execute("CREATE TABLE log (time text, level text, sessionid text, message text)")
    cursor.close()
    conn.commit()


if __name__ == '__main__':
    create_db(DB)
    populate_db(DB, DOTNET_LOG)
    conn = get_connection(DB)

    print(process_log_line(SAMPLE_LINE))


