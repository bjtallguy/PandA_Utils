__author__ = 'BJ'

import re
import csv
import sqlite3


PRINT_ALL_MESSAGES = False
FAIL_SESSIONS_ONLY = False

DOTNET_LOG = r"//sbsfiles0/IQA_SQA_Central/IQA - Automation/tmp/emortgages/dotnetlogs.csv"
WEBLOAD_TEST_LOG_1 = r"//sbsfiles0/IQA_SQA_Central/IQA - Automation/tmp/emortgages/test_logs1.csv"
WEBLOAD_TEST_LOG_2 = r"//sbsfiles0/IQA_SQA_Central/IQA - Automation/tmp/emortgages/test_logs1.csv"
OUT_FILE = r"//sbsfiles0/IQA_SQA_Central/IQA - Automation/tmp/emortgages/report.txt"

DB = 'dotnet.db'


dot_net_log_msgs = {}
web_load_test_msgs = []


def get_connection():
    connection = sqlite3.connect(DB)
    connection.row_factory = sqlite3.Row
    return connection


def reset_db(connection):
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS dotnetmsgs")
    cursor.execute("CREATE TABLE dotnetmsgs(time text, sessionid text, app_server text, web_server text,"
                   "message text, test_id text, outcome text)")
    import_dotnet_msgs_to_sql(cursor, DOTNET_LOG)
    import_test_log(cursor, WEBLOAD_TEST_LOG_1)
    import_test_log(cursor, WEBLOAD_TEST_LOG_2)
    cursor.close()
    connection.commit()


def import_test_log(cursor, log_file):
    with open(log_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            if row['SessionID'] != "EMPTY":
                cursor.execute('''UPDATE dotnetmsgs SET test_id=?, outcome=? WHERE sessionid=?''',
                               (row['$WebTestIteration'].strip(), row['Outcome'].strip(), row['SessionID'].strip()))


def import_dotnet_msgs_to_sql(cursor, log_file):
    def cleanup_message(m: str):
        m = re.sub(r'\s*Funder:.*$', '', m)
        m = re.sub(r'\s*SessionID:\w+', '', m)
        return m

    with open(log_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, dialect='excel')
        for row in reader:
            message = row[' Message'].strip()
            found = re.findall(r'(?<=Website IPAddress:)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', message)
            if found:
                web_server = found[0]
            else:
                web_server = ""
            message = cleanup_message(message)

            cursor.execute("INSERT INTO dotnetmsgs (time, app_server, web_server, sessionid, message, test_id, outcome)"
                           "VALUES (?, ?, ?, ?, ?, ?, ?)",
                           (row['Event Time'].strip(), row[' Machine Name'].strip(), web_server,
                            row[' Session ID'].strip(), message, "Not Known", "Not Known"))


def is_exception(message: str):
    if "Exception" in message:
        return True


if __name__ == '__main__':
    # reset_db(get_connection())

    c = get_connection().cursor()
    f = open(OUT_FILE, mode='w')

    if FAIL_SESSIONS_ONLY:
        where_clause = " WHERE outcome='Fail'"
        is_fail = " only contains failed tests, and"
    else:
        where_clause = ""
        is_fail = ""

    uid_count = c.execute("SELECT COUNT(DISTINCT sessionid) AS 'uid_count' FROM dotnetmsgs" + where_clause
                          ).fetchone()['uid_count']
    print("\n\n---------------------------------------------------", file=f)
    print(" This report%s includes %s tests" % (is_fail, uid_count), file=f)

    for uid in c.execute("SELECT DISTINCT sessionid FROM dotnetmsgs ORDER BY test_id" + where_clause).fetchall():
        msg_count = c.execute("SELECT COUNT(message) AS msg_count FROM dotnetmsgs WHERE sessionid=?",
                              (uid['sessionid'], )).fetchone()['msg_count']
        messages = c.execute("SELECT * FROM dotnetmsgs WHERE sessionid=? ORDER BY time", (msg['sessionid'], ))
        msg = messages.fetchone()
        print("\n\n---------------------------------------------------", file=f)
        print("Test ID:%s - %s : (SessionID:%s  App Server:%s  Web Server:%s  Messages:%s)" %
              (msg['test_id'], msg['outcome'], uid['sessionid'], msg['app_server'], msg['web_server'], msg_count),
              file=f)

        print(" - Service Layer invoked at %s\n" % (msg['time'][-5:], ), file=f)

        for msg in messages.fetchall():
            m = msg['message']
            if is_exception(m):
                print("%s - %s" % (msg['time'][-5:], m), file=f)
            if PRINT_ALL_MESSAGES:
                print("%s - %s" % (msg['time'][-5:], m), file=f)

    print("\n\n---------------------------------------------------", file=f)
    print("\n This report contains %s test runs." % uid_count, file=f)
    print("\n\n---------------------------------------------------", file=f)

    c.close()
