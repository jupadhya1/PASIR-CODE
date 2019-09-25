from subprocess import call
import os
import shutil
import uuid
import re
import tarfile
import datetime
import json
import logging
import requests
from threading import Thread
from werkzeug import secure_filename
import logging.handlers
import fnmatch
from multiprocessing.dummy import Pool as ThreadPool

import persistence
import config
import model_registry

db = persistence.db()
c = db.cursor()
_pool = ThreadPool(config.PARALLEL_JOBS)

sql = """CREATE TABLE IF NOT EXISTS CLASSIFIER (
      uid TEXT PRIMARY KEY NOT NULL,
      type TEXT,
      title TEXT,
      enabled INT,
      language TEXT,
      test_accuracy REAL,
      training_set_size INT,
      created_on TIMESTAMP,
      local_created_on TIMESTAMP,
      finished_on TIMESTAMP,
      state TEXT
      );"""
c.execute(sql)
sql = """CREATE TABLE IF NOT EXISTS RESOURCE (
      uid TEXT PRIMARY KEY NOT NULL,
      type TEXT,
      title TEXT,
      created_on TIMESTAMP,
      local_created_on TIMESTAMP,
      path TEXT
      );"""
c.execute(sql)
sql = """CREATE TABLE IF NOT EXISTS CLASSIFIER_RESOURCE (
      classifier_uid TEXT,
      key TEXT,
      resource_uid TEXT,
      PRIMARY KEY (classifier_uid, key, resource_uid),
      FOREIGN KEY(classifier_uid) REFERENCES CLASSIFIER(uid),
      FOREIGN KEY(resource_uid) REFERENCES RESOURCE(uid)
      );"""
c.execute(sql)
sql = """CREATE TABLE IF NOT EXISTS CLASSIFIER_META (
      classifier_uid TEXT,
      key TEXT,
      value TEXT,
      PRIMARY KEY (classifier_uid, key),
      FOREIGN KEY(classifier_uid) REFERENCES CLASSIFIER(uid)
      );"""
c.execute(sql)
sql = """CREATE TABLE IF NOT EXISTS JOB (
      uid TEXT,
      dir_name TEXT,
      created_on TIMESTAMP,
      classifier_uid TEXT,
      status TEXT,
      progress_percentage REAL,
      progress_text TEXT,
      PRIMARY KEY (uid),
      FOREIGN KEY(classifier_uid) REFERENCES CLASSIFIER(uid)
      );"""
c.execute(sql)
db.commit()

def sanitize_file_name(str) :
   valid_chars = '\'\-_\.\(\)\@\_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
   return re.sub('[^%s]' % valid_chars, '-', str)

class StoreJsonEncoder(json.JSONEncoder):
   def default(self, obj):
      if isinstance(obj, Resource): return obj.__dict__
      if isinstance(obj, Classifier): return obj.__dict__
      if isinstance(obj, JobContext): return obj.__dict__
      if isinstance(obj, logging.Logger): return ''
      return json.JSONEncoder.default(self, obj)

class Remote:
   def __init__(self, url, key):
      self.url = url
      self.key = key
      # TODO: do some handshake to test connection and key
   
   def invoke_jsonrpc(self, method, params=[]):
      headers = {'content-type': 'application/json'}
      request_uid = str(uuid.uuid1())
      payload = {
         "method": method,
         "params": params,
         "jsonrpc": "2.0",
         "id": request_uid,}
      if (self.key == ''):
         response = requests.post(
            self.url, data=json.dumps(payload), headers=headers).json()
      else:
         response = requests.post(
            self.url, data=json.dumps(payload), headers=headers, auth=('api', self.key)).json()
      assert response["jsonrpc"]
      assert response["id"] == request_uid
      return response["result"]

   def get_all_classifiers(self):
      json_str = self.invoke_jsonrpc('classifier.get_all')
      received_classifiers = json.loads(json_str)
      classifiers = []
      for classifier_entries in received_classifiers:
         classifiers.append(Classifier(entries=classifier_entries))
      return classifiers

   def fetch_resource(self, uid):
      if Resource.exists(uid): 
         raise Exception('Resource uid %s already exists locally' % uid)
      # fetch remote resource details
      resource_json = self.invoke_jsonrpc('resource.get', [uid])
      resource_dict = json.loads(resource_json)
      # download remote resource data to a local temp file
      local_filename = os.path.join(config.TEMP_PATH, 'remote-%s.tar.gz' % uid)
      if (self.key == ''):
         r = requests.get('%s/resource.download/%s' % (self.url, uid), stream=True)
      else:
         r = requests.get('%s/resource.download/%s' % (self.url, uid), stream=True, auth=('api', self.key))
      if r.status_code == 200:
         with open(local_filename, 'wb') as f:
           for chunk in r.iter_content(chunk_size=1024): 
              if chunk: f.write(chunk)
      elif r.status_code == 401:
         raise Exception('API-key verification failed with %s using key="%s"' % (self.url, self.key))
      else:
         raise Exception('Downloading remote resource from %s/resource.download/%s has failed' % (self.url, uid))
      # add resource locally
      resource = Resource.add_from_targz(
         resource_type = resource_dict['resource_type'], 
         title = resource_dict['resource_type'], 
         gz_path = local_filename, 
         uid = uid, 
         created_on = resource_dict['created_on'])
      # remove local temp file
      os.remove(local_filename)
      # return new local resource
      return resource
   
   def fetch_classifier(self, uid):
      if Classifier.exists(uid): 
         raise Exception('Classifier uid %s already exists locally' % uid)
      # fetch remote classifier object
      json_str = self.invoke_jsonrpc('classifier.get', [uid])
      received_classifier = json.loads(json_str)
      classifier = Classifier(entries=received_classifier)
      # check for resource dependencies
      for key, resource_uid in classifier.resources.iteritems():
         if not Resource.exists(resource_uid):
            # fetch+add resource locally
            self.fetch_resource(resource_uid)
      # save classifier object locally
      classifier.save()



class Resource:
   def __init__(self, entries=None): 
      if not entries==None: self.__dict__.update(entries)
   
   def __str__(self): return '<Resource %s>' % self.__dict__

   @classmethod
   def add(cls, uid, resource_type, title, created_on, local_created_on, path):
      db = persistence.db()
      c = db.cursor()
      # check existence
      c.execute('SELECT * FROM RESOURCE WHERE uid=?', [uid])
      if c.fetchone() != None: 
         raise Exception('Cannot add resource %s because it already exists' % uid)
      # create and fill object
      resource = Resource()
      resource.uid = uid
      resource.resource_type = resource_type
      resource.title = title
      resource.created_on = created_on
      resource.local_created_on = local_created_on
      resource.path = path
      # insert to DB
      try:
         c.execute("""INSERT INTO RESOURCE (
            uid, 
            type, 
            title, 
            created_on, 
            local_created_on,
            path) VALUES (?,?,?,?,?,?)""",
            [uid, resource_type, title, created_on, local_created_on, path])
         db.commit()
      except:
         raise Exception('Failed to insert resource %s into DB' % uid)
      # return new object
      logging.info('Inserted new resource %s' % uid)
      return resource
   
   @classmethod
   def get_all(cls, uid=''):
      c = persistence.cursor()
      if (uid==''):
         c.execute("""SELECT 
            uid,
            type,
            title,
            created_on,
            local_created_on,
            path 
            FROM RESOURCE""")
      else:
         c.execute("""SELECT 
            uid,
            type,
            title,
            created_on,
            local_created_on,
            path 
            FROM RESOURCE 
            WHERE UID=?""", [uid])
      resources = []
      for resource_t in c.fetchall():
         resource = Resource()
         resource.uid = resource_t[0]
         resource.resource_type = resource_t[1]
         resource.title = resource_t[2]
         resource.created_on = resource_t[3]
         resource.local_created_on = resource_t[4]
         resource.path = resource_t[5]
         resources.append(resource)
      return resources
   
   @classmethod
   def get(cls, uid): 
      result = Resource.get_all(uid)
      if len(result) == 0: return None
      else: return result[0]
   
   @classmethod
   def exists(cls, uid):
      c = persistence.cursor()
      c.execute("""SELECT * FROM RESOURCE WHERE UID=?""", [uid])
      return (c.fetchone() != None)
   
   @classmethod
   def add_from_targz(cls,
      resource_type, 
      title, 
      gz_path, 
      uid='', 
      created_on=str(datetime.datetime.now())):
      # generate uid (if not specified)
      logging.info('Attempting to add new resource from tar.gz: %s' % gz_path)
      if Resource.exists(uid): raise Exception('Resource uid %s already exists' % uid)
      if uid == '': uid = str(uuid.uuid1())
      # generate resource dir name
      resource_dir = '%s-%s' % (sanitize_file_name(resource_type), sanitize_file_name(uid))
      # open tar.gz
      try: 
         archive = tarfile.open(gz_path, 'r:gz')
      except: 
         raise Exception('Cannot add resource because specified gz_path %s is not accessible or corrupt' % gz_path)
      # extract given tar.gz to temp or final location
      tar_dir = archive.getnames()[0]
      try:
         if tar_dir == resource_dir: 
            # extract files to final location
            archive.extractall(config.RESOURCES_PATH)
         else:
            archive.extractall(config.TEMP_PATH)
            # move extracted files from temp location
            os.rename(os.path.join(config.TEMP_PATH, tar_dir), 
               os.path.join(config.RESOURCES_PATH, resource_dir))
      except:
         raise Exception('Failed to extract resource archive %s' % gz_path)
      # insert data and return object
      return Resource.add(uid=uid, 
            resource_type=resource_type, 
            title=title, 
            created_on=created_on, 
            local_created_on=str(datetime.datetime.now()), 
            path=resource_dir)
      
   @classmethod
   def add_from_file(cls,
      resource_type, 
      title, 
      file_path, 
      uid='', 
      created_on=str(datetime.datetime.now())):
      logging.info('Attempting to add new resource from file: %s' % file_path)
      # generate uid (if not specified)
      if uid == '': uid = str(uuid.uuid1())
      if Resource.exists(uid): raise Exception('Resource uid %s already exists' % uid)
      # generate resource dir name
      resource_dir = sanitize_file_name(resource_type) +'-'+ sanitize_file_name(uid)
      resource_dir_path = os.path.join(config.RESOURCES_PATH, resource_dir)
      try:
         if not os.path.exists(resource_dir_path): os.makedirs(resource_dir_path)
      except:
         raise Exception('Failed to create resource destination %s for resource %s' % (resource_dir_path, uid))
      try:
         shutil.copyfile(file_path, 
            os.path.join(resource_dir_path, os.path.basename(file_path)))
         # insert data and return object
         return Resource.add(uid=uid, 
            resource_type=resource_type, 
            title=title, 
            created_on=created_on, 
            local_created_on=str(datetime.datetime.now()), 
            path=resource_dir)
      except:
         shutil.rmtree(resource_dir_path)
         raise Exception('Failed to copy resource file %s to %s for resource %s' % (file_path, resource_dir_path, uid))        
   
   @classmethod
   def cleanup(cls):
      """Remove unreferenced resource directories"""
      # TODO
   
   def to_json(self):
      return json.dumps(self, default=lambda o: o.__dict__, indent=3)
   
   def to_targz(self):
      """Takes an existing resource and prepares a <uid>.tar.gz from the directory
      content under a temp location"""
      resource_path = '%s/%s' % (config.RESOURCES_PATH, self.path)
      out_path = "%s/%s.tar.gz" % (config.TEMP_PATH, self.uid)
      # TODO: check if tar.gz already exists and is consistent to speed up repeated calls
      with tarfile.open(out_path, "w:gz") as tar:
           tar.add(resource_path, arcname=os.path.basename(resource_path))
      return out_path
   
   def remove(self):
      db = persistence.db()
      c = db.cursor()
      c.execute("""SELECT * FROM CLASSIFIER_RESOURCE WHERE resource_uid=?""", [self.uid])
      if c.fetchone() != None: raise Exception('Cannot delete resource %s because of existing dependencies' % (self.uid))
      try: shutil.rmtree(os.path.join(config.RESOURCES_PATH, self.path))
      except: pass
      c.execute('DELETE FROM RESOURCE WHERE UID=?', [self.uid])
      db.commit()
      logging.info('Removed resource %s' % self.uid)
   

class Classifier:
   
   def __init__(self, entries=None): 
      if not entries==None: self.__dict__.update(entries)
      self.saved = False
   
   def __str__(self): return '<Classifier %s>' % self.__dict__
   
   @classmethod
   def add(cls, model_type, title, enabled, language, test_accuracy,
      training_set_size, resources, meta, uid='', 
      created_on=str(datetime.datetime.now()), finished_on=None, 
      state='Created'):
      if uid == '': uid = str(uuid.uuid1())
      # create and fill object
      classifier = Classifier()
      classifier.uid = uid
      classifier.model_type = model_type
      classifier.title = title
      classifier.enabled = enabled
      classifier.language = language
      classifier.test_accuracy = test_accuracy
      classifier.training_set_size = training_set_size
      classifier.created_on = created_on
      classifier.finished_on = finished_on
      classifier.local_created_on = str(datetime.datetime.now())
      classifier.state = state
      classifier.resources = resources
      classifier.meta = meta
      classifier.save()
      return classifier
   
   @classmethod
   def get_all(cls, uid=''): 
      c = persistence.cursor()
      if (uid==''):
         c.execute("""SELECT 
            uid,
            type,
            title,
            enabled,
            language,
            test_accuracy,
            training_set_size,
            created_on,
            finished_on,
            local_created_on,
            state
            FROM CLASSIFIER""")
      else:
         c.execute("""SELECT 
            uid,
            type,
            title,
            enabled,
            language,
            test_accuracy,
            training_set_size,
            created_on,
            finished_on,
            local_created_on,
            state 
            FROM CLASSIFIER
            WHERE uid=?""", [uid])
      classifiers = []
      for c_t in c.fetchall():
         cl = Classifier()
         cl.uid = c_t[0]
         cl.model_type = c_t[1]
         cl.title = c_t[2]
         cl.enabled = True if c_t[3]==1 else False
         cl.language = c_t[4]
         cl.test_accuracy = c_t[5]
         cl.training_set_size = c_t[6]
         cl.created_on = c_t[7]
         cl.finished_on = c_t[8]
         cl.local_created_on = c_t[9]
         cl.state = c_t[10]
         cl.resources = {}
         c.execute("""SELECT key, resource_uid 
                      FROM CLASSIFIER_RESOURCE 
                      WHERE classifier_uid=?""",[cl.uid])
         for r in c.fetchall():
            cl.resources[r[0]] = r[1]
         cl.meta = {}
         c.execute("""SELECT key, value 
                      FROM CLASSIFIER_META
                      WHERE classifier_uid=?""",[cl.uid])
         for m in c.fetchall():
            cl.meta[m[0]] = m[1]
         cl.saved = True
         classifiers.append(cl)
      return classifiers
   
   @classmethod
   def get(cls, uid):
      result = Classifier.get_all(uid)
      if len(result) == 0: return None
      else: return result[0]
   
   @classmethod
   def exists(cls, uid):
      c = persistence.cursor()
      c.execute("""SELECT * FROM CLASSIFIER WHERE uid=?""", [uid])
      return (c.fetchone() != None)

   # @classmethod
   # def from_json(cls, json_str):
   #    classifier = Classifier(json.loads(json_str))
   #    return classifier
   
   def save(self):
      db = persistence.db()
      c = db.cursor()
      if self.saved: return
      if Classifier.exists(self.uid): raise Exception('Classifier uid %s exists already' % self.uid)
      # check existence of resources
      for key, resource_uid in self.resources.iteritems():
         if not Resource.exists(resource_uid):
            raise Exception('Required resource %s of Classifier %s does not exist locally' % (resource_uid, self.uid))
      # instert into DB
      try:
         c.execute("""INSERT INTO CLASSIFIER (
            uid,
            type,
            title,
            enabled,
            language,
            test_accuracy,
            training_set_size,
            created_on,
            finished_on,
            local_created_on,
            state) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            [self.uid, 
            self.model_type, 
            self.title, 
            self.enabled,
            self.language,
            self.test_accuracy,
            self.training_set_size,
            self.created_on, 
            self.finished_on,
            self.local_created_on,
            self.state])
         for key, resource_uid in self.resources.iteritems():
            c.execute("""INSERT INTO CLASSIFIER_RESOURCE (
                  classifier_uid,
                  key,
                  resource_uid)
                  VALUES (?,?,?)""",
                  [self.uid, key, resource_uid])
         for key, value in self.meta.iteritems():
            c.execute("""INSERT INTO CLASSIFIER_META (
                  classifier_uid,
                  key,
                  value)
                  VALUES (?,?,?)""",
                  [self.uid, key, value])
         db.commit()
      except: 
         db.rollback()
         raise Exception('Failed to insert classifier %s into DB' % self.uid)
      self.saved = True
      logging.info('Inserted new classifier %s' % self.uid)
   
   def to_json(self):
      return json.dumps(self, default=lambda o: o.__dict__, indent=3)
   
   def trained(self): return (self.finished_on != None)

   def set_enabled(self, enabled=True):
      db = persistence.db()
      c = db.cursor()
      self.enabled = enabled
      try: 
         c.execute("UPDATE CLASSIFIER SET enabled=? WHERE uid=?", 
            [self.enabled, self.uid])
         db.commit()
      except:
         db.rollback()
         raise Exception('Failed to update classifier %s as enabled=%s' % (self.uid, self.enabled))
      logging.info('Set classifier enabled=%s %s' % (enabled, self.uid))

   def remove(self):
      db = persistence.db()
      c = db.cursor()
      c.execute('DELETE FROM CLASSIFIER_META WHERE classifier_uid=?', [self.uid])
      c.execute('DELETE FROM CLASSIFIER_RESOURCE WHERE classifier_uid=?', [self.uid])
      c.execute('DELETE FROM CLASSIFIER WHERE uid=?', [self.uid])
      db.commit()
      logging.info('Removed classifier %s' % self.uid)

   def train(self, 
             job, 
             in_csv, 
             in_desc_col, 
             in_res_col, 
             in_class_col):
      db = persistence.db()
      c = db.cursor()
      if not self.saved: self.save()
      if self.trained(): 
         raise Exception('Model %s is already trained' % self.uid)
      # set training start
      logging.info('Starting to train classifier %s' % self.uid)
      self.state = 'Training'
      try: 
         c.execute("UPDATE CLASSIFIER SET state=? WHERE uid=?", 
            [self.state, self.uid])
         db.commit()
      except:
         db.rollback()
         raise Exception('Failed to update classifier %s as training started' % self.uid)
      # run training
      # TODO
      # modelregistry.train(type, meta, resources, in_csv, in_text_col, in_class_col)
      # (...............)
      # set training end
      self.state = 'Ready'
      self.finished_on = str(datetime.datetime.now())
      try: 
         c.execute("UPDATE CLASSIFIER SET finished_on=?, state=? WHERE uid=?", 
            [self.finished_on, self.state, self.uid])
         db.commit()
      except:
         db.rollback()
         raise Exception('Failed to update classifier %s as training started' % self.uid)
      logging.info('Finished training classifier %s' % self.uid)

   def classify(self, 
                job_context, 
                in_csv, 
                in_desc_col, 
                in_res_col, 
                out_csv, 
                out_class_col):
      # db = persistence.db()
      # c = db.cursor()
      if not self.saved: self.save()
      if not self.trained: 
         raise Exception('Model %s is not yet finished training' % self.uid)
      if not self.enabled: 
         raise Exception('Model %s is not enabled' % self.uid)
      logging.info('Executing classifier %s' % self.uid)
      resource_paths = {}
      for key, resource_uid in self.resources.iteritems():
         resource_paths[key] = os.path.join(config.RESOURCES_PATH, Resource.get(resource_uid).path)
      # do sh.t on separate thread and return immediately
      # Thread(target = model_registry.classify, args = (
      #    job_context,
      #    self.model_type,
      #    self.meta,
      #    resource_paths,
      #    in_csv,
      #    in_desc_col,
      #    in_res_col,
      #    out_csv,
      #    out_class_col)).start()
      a=_pool.apply_async(func = model_registry.classify, args = (
         job_context,
         self.model_type,
         self.meta,
         resource_paths,
         in_csv,
         in_desc_col,
         in_res_col,
         out_csv,
         out_class_col))
      Thread(target = a.get).start()
      return


class JobContext():

   def __init__(self, 
      uid, 
      dir_name, 
      created_on, 
      classifier_uid, 
      status='Scheduled',
      progress_percentage=0,
      progress_text=''):
      self.uid = uid
      self.dir_name = dir_name
      self.created_on = created_on
      self.classifier_uid = classifier_uid
      self.status = status
      self.progress_percentage = progress_percentage
      self.progress_text = progress_text
      self.aux_progress_callback = None
      # create job work dir
      self.work_dir = os.path.join(config.WORK_PATH, 
         secure_filename(self.dir_name))
      if not os.path.exists(self.work_dir): os.makedirs(self.work_dir)
      # set up job logging (add handlers only at creation)
      self.logger = logging.getLogger(self.uid)
      if not len(self.logger.handlers):
         handler = logging.FileHandler(
            os.path.join(self.work_dir, 'progress.log'))
         handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s'))
         self.logger.addHandler(handler)

   @classmethod
   def create(cls, classifier_uid):
      db = persistence.db()
      c = db.cursor()
      # generate uid
      uid = str(uuid.uuid1())
      dir_name = '%s-%s' % (
         datetime.datetime.now().strftime('%Y%m%d%H%M%S'), uid)
      # create and fill object
      job = JobContext(uid = uid,
         dir_name = dir_name,
         created_on = str(datetime.datetime.now()),
         classifier_uid = classifier_uid)
      job.save()
      job.logger.info('Created job %s' % uid)
      return job
   
   @classmethod
   def exists(cls, uid):
      c = persistence.cursor()
      c.execute("""SELECT * FROM JOB WHERE UID=?""", [uid])
      return (c.fetchone() != None)
   
   @classmethod
   def get(cls, uid):
      result = JobContext.get_all(uid)
      if len(result) == 0: return None
      else: return result[0]
   
   @classmethod
   def get_all(cls, uid=''):
      c = persistence.cursor()
      if (uid==''):
         c.execute("""SELECT 
                   uid,
                   dir_name,
                   created_on,
                   classifier_uid,
                   status,
                   progress_percentage,
                   progress_text
                   FROM JOB""")
      else:
         c.execute("""SELECT 
                   uid,
                   dir_name,
                   created_on,
                   classifier_uid,
                   status,
                   progress_percentage,
                   progress_text
                   FROM JOB
                   WHERE UID=?""", [uid])
      jobs = []
      for job_t in c.fetchall():
         job = JobContext(
            uid = job_t[0],
            dir_name = job_t[1],
            created_on = job_t[2],
            classifier_uid = job_t[3],
            status=job_t[4],
            progress_percentage=job_t[5],
            progress_text=job_t[6])
         jobs.append(job)
      return jobs
   
   def update_progress(self, percentage, text, status='Progress'):
      if not status == 'Error':
         self.logger.info('JobProgress:%d %s' % (percentage, text))
      else:
         self.logger.error('JobProgress:%d %s' % (percentage, text))
      self.status = status
      self.progress_percentage = percentage
      self.progress_text = text
      self.save()
      if self.aux_progress_callback != None:
         self.aux_progress_callback(percentage, text, status)

   def mark_done(self):
      self.update_progress(100, 'Done', 'Done')

   def remove(self):
      db = persistence.db()
      c = db.cursor()
      try: shutil.rmtree(self.work_dir)
      except: pass
      c.execute('DELETE FROM JOB WHERE UID=?', [self.uid])
      db.commit()
      logging.info('Removed job %s' % self.uid)
   
   def save(self):
      db = persistence.db()
      c = db.cursor()
      if JobContext.exists(self.uid): 
         try:
            c.execute("""UPDATE JOB SET
               status=?,
               progress_percentage=?,
               progress_text=?
               WHERE uid=?""",
               [self.status,
               self.progress_percentage,
               self.progress_text,
               self.uid])
            db.commit()
         except: 
            db.rollback()
            raise Exception('Failed to update job status of %s' % self.uid)
      else:
         # instert into DB
         try:
            c.execute("""INSERT INTO JOB (
               uid,
               dir_name,
               created_on,
               classifier_uid,
               status,
               progress_percentage,
               progress_text)
               VALUES (?,?,?,?,?,?,?)""",
               [self.uid, 
               self.dir_name, 
               self.created_on, 
               self.classifier_uid,
               self.status,
               self.progress_percentage,
               self.progress_text])
            db.commit()
         except: 
            db.rollback()
            raise Exception('Failed to insert job %s' % self.uid)
   