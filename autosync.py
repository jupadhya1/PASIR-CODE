from threading import Thread, Timer
import time
import logging

import store

class AutosyncThread(Thread):
   
   def __init__(self, name, url, key, sync_model_types, interval_sec):
      Thread.__init__(self)
      self.name = name
      self.url = url
      self.sync_model_types = sync_model_types
      self.interval_sec = interval_sec
      self.setDaemon(True)
      self.remote = store.Remote(url, key)
      self.first_sync = True
   
   def run(self):
      logging.info('Setting up Autosync [%s] with %s for %s models' % (self.name, self.url, str.join(', ', self.sync_model_types)))
      while True:
         self.sync()
         logging.info('Autosync [%s] scheduled next sync in %d hours' % (self.name, self.interval_sec/3600))
         time.sleep(self.interval_sec)

   def sync(self):
      try:
         # wait a bit, if running right after server startup
         if self.first_sync: 
            time.sleep(7)
            self.first_sync = False
         # start
         to_fetch = []
         logging.info('Autosync [%s] starts syncing now' % self.name)
         # fetch list of remote classifiers
         remote_classifiers = self.remote.get_all_classifiers()
         for remote_classifier in remote_classifiers:
            found = True
            # if type is matching, classifier is trained and enabled, 
            # and doesn't exist locally, queue for fetching
            if ((remote_classifier.model_type in self.sync_model_types) and 
            	remote_classifier.trained() and 
            	(not store.Classifier.exists(remote_classifier.uid)) and 
            	remote_classifier.enabled):
            	to_fetch.append(remote_classifier)
         if (len(to_fetch) == 0):
            logging.info('Autosync [%s] found everything up-to-date' % self.name)
         else:
            logging.info('Autosync [%s] found %d new classifier(s) to fetch' % (self.name, len(to_fetch)))
            # fetch queued classifiers
            for remote_classifier in to_fetch:
               logging.info('Autosync [%s] starts to fetch classifier %s' % (self.name, remote_classifier.uid))
               self.remote.fetch_classifier(remote_classifier.uid)
      except Exception as e:
         logging.exception('Autosync [%s] encountered an error, exits syncing' % self.name)



