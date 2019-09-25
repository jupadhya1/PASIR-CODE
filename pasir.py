import pandas as pd
import numpy as np
import jaydebeapi as jdbc #LGPL :(
import jpype
import datetime
import time
import logging
import os
from threading import Thread, Condition, Semaphore
import time
import math
import csv

import config
import store

_keep_alive_sql = 'SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1'
_ticket_insert_semaphore = Semaphore(1)

class _DB2ConnectionFactoryThread(Thread):
# this helper class is required to work around an issue in jaydebeapi
# that prevents subsequent creation of jdbc connections from threads other 
# than from which the first connection instance was created (assumably because
# of a flawed way of java classloading in jpype)
   
   def __init__(self, 
      jvm_path, 
      jvm_args, 
      jdbc_class, 
      db2_url, 
      db2_user, 
      db2_pass):
      Thread.__init__(self)
      self.setDaemon(True)
      self.condition = Condition()
      self.conn = None
      self.exception = None
      self.jvm_path = jvm_path
      self.jvm_args = jvm_args
      self.jdbc_class = jdbc_class
      self.db2_url = db2_url
      self.db2_user = db2_user
      self.db2_pass = db2_pass
   
   # makes sure connection creation always runs on the same thread
   def run(self):
      while True:
         self.condition.acquire()
         while not (self.conn == None and self.exception == None):
            self.condition.wait()
         # if notified and conn is None, create new connection
         logging.info('Connecting DB2 at %s as (%s:***)' % (self.db2_url, self.db2_user))
         try:
            if not jpype.isJVMStarted():
               jpype.startJVM(self.jvm_path, self.jvm_args)
            # open connection
            self.conn = jdbc.connect(self.jdbc_class, 
               [self.db2_url, self.db2_user, self.db2_pass])
            self.conn.jconn.setAutoCommit(True)
         except Exception as e:
            # shallow exception and let get_new_connection raise it
            self.exception = e
            pass
         self.condition.release()
   
   # invokeable by any thread, returns a new connection instance correctly
   def get_new_connection(self):
      self.exception = None
      self.conn = None
      self.condition.acquire()
      self.condition.notify()
      self.condition.release()
      while self.conn == None and self.exception == None: 
         time.sleep(0.3) # we could in fact use another Condition instead of this...
      if not self.exception == None: raise self.exception 
      return self.conn

if config.JVM_PATH == '':
   _JVM_PATH = jpype.getDefaultJVMPath()
else:
   _JVM_PATH = config.JVM_PATH

# instantiate connection factory at module load
_db2_connection_factory = _DB2ConnectionFactoryThread(
   _JVM_PATH,
   config.JVM_ARGS,
   config.JDBC_CLASS,
   config.DB2_URL,
   config.DB2_USER,
   config.DB2_PASS)
_db2_connection_factory.start()
_conn = None

# returns an alive, thread-safe connection instance to the PASIR DB2 database anytime
def pasir_db():
   global _conn, _db2_connection_factory
   if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
      jpype.attachThreadToJVM()
   # check if existing connection is alive
   if not _conn == None:
      try:
         # execute dummy query - assuming autocommit, this has no side-effect
         _conn.cursor().execute(_keep_alive_sql)
      except:
         _conn = None
   # if no connection yet (or not alive anymore), create one
   if _conn == None:
      # prepare jvm using config.JVM_ARGS to load jdbc driver
      _conn = _db2_connection_factory.get_new_connection()
   return _conn

# use for selects, or inserts when you need the inserted ids
def sql_to_data_frame(sql, 
   params=[], 
   logger=logging, 
   verbose=True):
   start = time.time()
   df = pd.read_sql(sql, pasir_db(), params=params)
   if verbose:
      if not logging == None: logger.info('SQL execution time: %f s' % (time.time()-start))
   return df

# use for updates and inserts (with no returned results)
def sql_execute(sql, 
   params=[], 
   logger=logging, 
   verbose=False):
   start = time.time()
   df = pasir_db().cursor().execute(sql, params)
   if verbose:
      if not logging == None: logger.info('SQL execution time: %f s' % (time.time()-start))   

# use to insert a pd.DataFrame with a specified sql
# the number, order and type of data_frame columns must match
# the field list specified in the insert sql
def sql_using_data_frame(sql, 
   data_frame, 
   field_order, 
   batch_size=1000, 
   logger=logging,
   verbose=True):
   start = time.time()
   # keep the inserted columns only
   data_frame = data_frame[field_order]
   # loop thru all valid batches
   for x in xrange(int(math.ceil(data_frame.shape[0]/batch_size)+1)): 
      # extract batch
      batch = data_frame[x*batch_size:(x+1)*batch_size]
      if verbose:
         if not logging == None: logger.info('Processing records %d - %d' % (x*batch_size+1, min((x+1)*batch_size, data_frame.shape[0])))   
      # convert data_frame into array of tuples, also convert any NaNs to None
      tuples = [tuple(row) for row in [[(None if pd.isnull(x) else x) for x in row] \
         for row in batch.values]]
      # execute
      pasir_db().cursor().executemany(sql, tuples)
   if verbose:
      if not logging == None: logger.info('Batch SQL execution time: %f s' % (time.time()-start))   


class PasirTicketClassification:
   
   def __init__(self, 
      job_context,
      classr_ticketclassification_id, 
      classifier,
      client_id, 
      data_source, 
      from_timestamp, 
      to_timestamp,
      created_on = str(datetime.datetime.now()),
      ticket_count = None):
      self.job_context = job_context
      self.classr_ticketclassification_id = classr_ticketclassification_id
      self.created_on = created_on
      self.classifier = classifier
      self.client_id = client_id
      self.data_source = data_source
      self.from_timestamp = from_timestamp
      self.to_timestamp = to_timestamp
      self.ticket_count = ticket_count
   
   # pass ts args as dt objects: datetime.datetime.strptime('2013-01-01','%Y-%m-%d')
   @classmethod
   def create(cls, 
      classifier,
      client_id, 
      data_source, 
      from_timestamp, 
      to_timestamp):
      # check params
      if not type(from_timestamp) == datetime.datetime: raise Exception('Cannot create new PasirTicketClassification, from_timestamp must be of type datetime')
      if not type(to_timestamp) == datetime.datetime: raise Exception('Cannot create new PasirTicketClassification, to_timestamp must be of type datetime')
      if classifier == None: raise Exception('Cannot create new PasirTicketClassification without specifying the classifier')
      if not classifier.enabled: raise Exception('Cannot create new PasirTicketClassification, classifier is not enabled: %s' % classifier.uid)
      if not classifier.title == 'PASIR': raise Exception('Cannot create new PasirTicketClassification, classifier title is "%s", expected "PASIR": %s' % [classifier.title, classifier.uid])
      # create job context
      job = store.JobContext.create(classifier.uid)
      # insert into CLASSR_TICKETCLASSIFICATION
      job.logger.info('Inserting CLASSR_TICKETCLASSIFICATION...')
      result = sql_to_data_frame(
         open(config.SQL_INSERT_TICKETCLASSIFICATION).read(),
         [classifier.uid,
            job.uid,
            client_id,
            data_source,
            str(from_timestamp),
            str(to_timestamp),
            'Ready to fetch tickets',
            0], 
         job.logger)
      db_id = result['ID'][0]
      # set up instance
      instance = PasirTicketClassification(
         job_context = job,
         classr_ticketclassification_id = db_id,
         classifier = classifier,
         client_id = client_id, 
         data_source = data_source, 
         from_timestamp = from_timestamp, 
         to_timestamp = to_timestamp)
      # hook progress callback
      job.aux_progress_callback = instance._update_progress
      # log some info
      job.logger.info('CLASSR_TICKETCLASSIFICATION.ID : %d' % db_id)
      job.logger.info('Client      : %s' % (instance.client_id))
      job.logger.info('Data source : %s' % (instance.data_source))
      job.logger.info('Date from   : %s' % (str(instance.from_timestamp)))
      job.logger.info('Date to     : %s (entire calendar day inclusive)' % (str(instance.to_timestamp)))
      job.logger.info('Ready to fetch')
      return instance
   
   # invokeable by external caller, returns asynchronously
   def fetch_and_classify(self):
      # fetch tickets from DB
      self.job_context.logger.info('Fetching tickets...')
      self._update_progress(1, 'Fetching tickets', 'Progress')
      in_tickets = sql_to_data_frame(
         sql = open(config.SQL_TICKETS_TO_CLASSIFY).read(),
         params = [self.data_source,
            self.client_id,
            str(self.from_timestamp),
            str(self.to_timestamp)])
      self.ticket_count = in_tickets.shape[0]
      self.job_context.logger.info('Fetched tickets:  %d' % self.ticket_count)
      # save tickets to CSV
      in_csv = os.path.join(self.job_context.work_dir, 'tickets.csv')
      in_tickets.to_csv(in_csv, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')
      self.job_context.logger.info('Tickets saved to %s' % in_csv)
      # update ticket count in the CLASSR_TICKETCLASSIFICATION record
      sql_execute(
         sql = open(config.SQL_UPDATE_TICKET_COUNT).read(),
         params = [self.ticket_count, 
            self.classr_ticketclassification_id], 
         logger = self.job_context.logger,
         verbose = False)
      # check if any tickets were pulled
      if self.ticket_count == 0:
         self.job_context.logger.info('No tickets to classify')
         self.job_context.mark_done()
         self._update_progress(100, 'No tickets to classify', 'Done')
      else:
         # invoke classifier
         self.classifier.classify(self.job_context, 
               in_csv, 
               'DESCRIPTION', 
               'RESOLUTION', 
               os.path.join(self.job_context.work_dir, 'classified-tickets.csv'), 
               'TICKETCLASS')
   
   # invoked by the job_context's callback hook
   def _update_progress(self, percentage, text, state):
      if state == 'Progress': state = 'Running'
      
      if percentage == 100 and \
         state != 'Error' and \
         self.ticket_count > 0: 
         state = 'Done'
         self.job_context.logger.info('Classifier finished successfully, processing results...')
         # insert classified tickets
         self._insert_classified_tickets()
      
      # update progress in CLASSR_TICKETCLASSIFICATION record
      sql_execute(
         sql = open(config.SQL_UPDATE_PROGRESS).read(),
         params = [state, 
            text,
            percentage,
            self.classr_ticketclassification_id], 
         logger = self.job_context.logger,
         verbose = False)
   
   # invoked by self._update_progress() when progress reaches 100%
   def _insert_classified_tickets(self):
      out_csv = os.path.join(self.job_context.work_dir, 
         'classified-tickets.csv')
      if not os.path.exists(out_csv):
         raise Exception('Classified tickets not found under %s' % out_csv)
      out_tickets = pd.read_csv(out_csv)
      # add missing DB columns
      out_tickets['CLASSR_TICKETCLASSIFICATION_ID'] = self.classr_ticketclassification_id
      out_tickets['CLEAN'] = 'Y'
      out_tickets['QUALITY_ISSUE'] = None
      self.job_context.logger.info('Checking data quality')
      # identify and mark data quality issues
      # empty clean.description
      out_tickets.loc[pd.isnull(out_tickets['clean.description']),'CLEAN'] = 'N'
      out_tickets.loc[pd.isnull(out_tickets['clean.description']),'QUALITY_ISSUE'] = \
         'Empty description after cleansing'
      # empty clean.resolution
      out_tickets.loc[pd.isnull(out_tickets['clean.resolution']),'CLEAN'] = 'N'
      out_tickets.loc[pd.isnull(out_tickets['clean.resolution']),'QUALITY_ISSUE'] = \
         'Empty resolution after cleansing'
      # empty resolution
      out_tickets.loc[pd.isnull(out_tickets['RESOLUTION']),'CLEAN'] = 'N'
      out_tickets.loc[pd.isnull(out_tickets['RESOLUTION']),'QUALITY_ISSUE'] = \
         'Empty resolution'
      # empty description
      out_tickets.loc[pd.isnull(out_tickets['DESCRIPTION']),'CLEAN'] = 'N'
      out_tickets.loc[pd.isnull(out_tickets['DESCRIPTION']),'QUALITY_ISSUE'] = \
         'Empty description'
      # empty description and resolution
      out_tickets.loc[(pd.isnull(out_tickets['DESCRIPTION'])) & 
      (pd.isnull(out_tickets['RESOLUTION'])),'CLEAN'] = 'N'
      out_tickets.loc[(pd.isnull(out_tickets['DESCRIPTION'])) & 
      (pd.isnull(out_tickets['RESOLUTION'])),'QUALITY_ISSUE'] = \
         'Empty description and resolution'
      # null out fields where data issue was identified (must use NaN for floats)
      out_tickets.loc[out_tickets['CLEAN']=='N','TICKETCLASS'] = None
      out_tickets.loc[out_tickets['CLEAN']=='N','Disk'] = np.NaN
      out_tickets.loc[out_tickets['CLEAN']=='N','Nonactionable'] = np.NaN
      out_tickets.loc[out_tickets['CLEAN']=='N','Other'] = np.NaN
      out_tickets.loc[out_tickets['CLEAN']=='N','Performance'] = np.NaN
      out_tickets.loc[out_tickets['CLEAN']=='N','Process'] = np.NaN
      out_tickets.loc[out_tickets['CLEAN']=='N','Server unavailable'] = np.NaN
      out_tickets.to_csv(out_csv, quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8')
      # insert classified tickets
      self.job_context.logger.info('Inserting classified tickets')
      _ticket_insert_semaphore.acquire()
      try:
         sql_using_data_frame(
            sql = open(config.SQL_INSERT_CLASSIFIED_TICKETS).read(),
            data_frame = out_tickets,
            field_order = [
               'CLASSR_TICKETCLASSIFICATION_ID',
               'ID',
               'TICKETCLASS',
               'Disk',
               'Nonactionable',
               'Other',
               'Performance',
               'Process',
               'Server unavailable',
               'CLEAN',
               'QUALITY_ISSUE'],
            batch_size = config.BATCH_SIZE,
            logger = self.job_context.logger,
            verbose = True)
         self.job_context.logger.info('Classified tickets inserted successfully')
      except Exception as e:
         self.job_context.logger.exception('An exception occured while classified tickets were tried to be insterted.')
         self._update_progress(100, 'Failed inserting tickets', 'Error')
      finally:
         _ticket_insert_semaphore.release()













