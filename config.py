from ConfigParser import SafeConfigParser
import socket
import os
import logging
import logging.handlers
import re
import sys

parser = SafeConfigParser()
config_files = parser.read(['config/defaults.cfg', 'config/%s.cfg' % socket.getfqdn()])

# [server]
try: ENABLE_TRAINING = parser.getboolean('server','ENABLE_TRAINING')
except: ENABLE_TRAINING = True

try: ENABLE_CLASSIFICATION = parser.getboolean('server','ENABLE_CLASSIFICATION')
except: ENABLE_CLASSIFICATION = True

try: ENABLE_RESOURCE_DOWNLOAD = parser.getboolean('server','ENABLE_RESOURCE_DOWNLOAD')
except: ENABLE_RESOURCE_DOWNLOAD = True

try: PARALLEL_JOBS = parser.getint('server','PARALLEL_JOBS')
except: PARALLEL_JOBS = 2

try: IP_MASK = parser.get('server','IP_MASK')
except: IP_MASK = '127.0.0.1'

try: PORT = parser.getint('server','PORT')
except: PORT = 8080

try: API_KEY = parser.get('server','API_KEY')
except: API_KEY = ''

try: SSL_ENABLED = parser.getboolean('server','SSL_ENABLED')
except: SSL_ENABLED = False

try: SSL_PRIVATE_KEY = parser.get('server', 'SSL_PRIVATE_KEY')
except: SSL_PRIVATE_KEY = ''
try: SSL_CERT = parser.get('server', 'SSL_CERT')
except: SSL_CERT = ''

if SSL_ENABLED and (SSL_CERT == '' or SSL_PRIVATE_KEY == ''):
	raise Exception('SSL is enabled in the config but no SSL_CERT and/or SSL_PRIVATE_KEY is specified')

try: CLEAN_JOBS_AFTER_DAYS = parser.getint('server','CLEAN_JOBS_AFTER_DAYS')
except: CLEAN_JOBS_AFTER_DAYS = 0

# [data]
try: LOG_PATH = parser.get('data','LOG_PATH')
except: LOG_PATH = 'logs'
if not os.path.exists(LOG_PATH): os.makedirs(LOG_PATH)

try: TEMP_PATH = parser.get('data','TEMP_PATH')
except: TEMP_PATH = '/tmp'
if not os.path.exists(TEMP_PATH): os.makedirs(TEMP_PATH)

try: RESOURCES_PATH = parser.get('data','RESOURCES_PATH')
except: RESOURCES_PATH = 'data/resources'
if not os.path.exists(RESOURCES_PATH): os.makedirs(RESOURCES_PATH)

try: WORK_PATH = parser.get('data','WORK_PATH')
except: WORK_PATH = 'data/work'
if not os.path.exists(WORK_PATH): os.makedirs(WORK_PATH)

# [pasir]
try: DB2_URL = parser.get('pasir','DB2_URL')
except: DB2_URL = ''

try: DB2_USER = parser.get('pasir','DB2_USER')
except: DB2_USER = ''

try: DB2_PASS = parser.get('pasir','DB2_PASS')
except: DB2_PASS = ''

try: JVM_PATH = parser.get('pasir','JVM_PATH')
except: JVM_PATH = ''

try: JVM_ARGS = parser.get('pasir','JVM_ARGS')
except: JVM_ARGS = ''

try: JDBC_CLASS = parser.get('pasir','JDBC_CLASS')
except: JDBC_CLASS = ''

try: SQL_TICKETS_TO_CLASSIFY = parser.get('pasir','SQL_TICKETS_TO_CLASSIFY')
except: SQL_TICKETS_TO_CLASSIFY = 'sql/select-tickets-to-classify.sql'

try: SQL_INSERT_TICKETCLASSIFICATION = parser.get('pasir','SQL_INSERT_TICKETCLASSIFICATION')
except: SQL_INSERT_TICKETCLASSIFICATION = 'sql/insert-ticketclassification.sql'

try: SQL_UPDATE_TICKET_COUNT = parser.get('pasir','SQL_UPDATE_TICKET_COUNT')
except: SQL_UPDATE_TICKET_COUNT = 'sql/update-classifier-ticket_count.sql'

try: SQL_UPDATE_PROGRESS = parser.get('pasir','SQL_UPDATE_PROGRESS')
except: SQL_UPDATE_PROGRESS = 'sql/update-classifier-progress.sql'

try: SQL_INSERT_CLASSIFIED_TICKETS = parser.get('pasir','SQL_INSERT_CLASSIFIED_TICKETS')
except: SQL_INSERT_CLASSIFIED_TICKETS = 'sql/insert-classified-tickets.sql'

try: SQL_TRAINING_DATA = parser.get('pasir','SQL_TRAINING_DATA')
except: SQL_TRAINING_DATA = 'sql/select-training-tickets.sql'

try: SQL_TEST_DATA = parser.get('pasir','SQL_TEST_DATA')
except: SQL_TEST_DATA = 'sql/select-test-tickets.sql'

try: BATCH_SIZE = parser.getint('pasir','BATCH_SIZE')
except: BATCH_SIZE = 1000


# set up console logging
logging.basicConfig(format='%(message)s', level=logging.DEBUG)
# set up rotating log file
rotated_handler = logging.handlers.RotatingFileHandler(
   filename=os.path.join(LOG_PATH, 'classr.log'), 
   mode='a', maxBytes=104857600, backupCount=10000) # 104857600==100MB
rotated_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
rotated_handler.setFormatter(formatter)
logging.getLogger('').addHandler(rotated_handler)

# set up stderr logging
class LoggerWriter:
   def __init__(self, level):
      # self.level is really like using log.debug(message)
      # at least in my case
      self.level = level

   def write(self, message):
      # if statement reduces the amount of newlines that are
      # printed to the logger
      if message != '\n': self.level(re.sub('(\\n)+$','',message))
   
   def flush(self):
      # create a flush method so things can be flushed when
      # the system wants to. Not sure if simply 'printing'
      # sys.stderr is the correct way to do it, but it seemed
      # to work properly for me.
      self.level(sys.stderr)
sys.stderr = LoggerWriter(logging.getLogger('').error)

logging.info('Reading configuration from: %s ' % config_files)

# [autosync:*]
AUTOSYNC = {}
# collect autosync config
try:
   for section in parser.sections():
      m = re.match('autosync:(\w+)', section)
      if not m == None:
         name = m.group(1)
         autosync_remote = {}
         autosync_remote['URL'] = parser.get(section,'URL')
         autosync_remote['KEY'] = parser.get(section,'API_KEY')
         autosync_remote['SYNC_MODEL_TYPES'] = parser.get(section,'SYNC_MODEL_TYPES').split(',')
         autosync_remote['SYNC_INTERVAL_HOURS'] = parser.getint(section,'SYNC_INTERVAL_HOURS') #*3600
         AUTOSYNC[name] = autosync_remote
except Exception as e:
   logging.error(e)