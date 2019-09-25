from threading import Thread, Timer
import time
import logging
import datetime

import store

class AutocleanThread(Thread):
   
   def __init__(self, clean_jobs_after_days):
      Thread.__init__(self)
      self.clean_jobs_after_days = clean_jobs_after_days
      self.setDaemon(True)
      self.interval_sec = 60*60*24 # 24 hours
   
   def run(self):
      logging.info('Setting up Autoclean for job data older than %d days' % 
         self.clean_jobs_after_days)
      while self.clean_jobs_after_days > 0:
         logging.info('Autoclean scheduled next cleanup in %d hours' % 
            (self.interval_sec/3600))
         time.sleep(self.interval_sec)
         self.cleanup()
   
   def cleanup(self):
      try:
         logging.info('Autoclean starts to clean old jobs now')
         # fetch list of job contexts
         job_contexts = store.JobContext.get_all()
         removed_jobs = 0
         for job in job_contexts:
            job_age = (datetime.datetime.now() - 
               datetime.datetime.strptime(str(job.created_on),
                  '%Y-%m-%d %H:%M:%S.%f'))
            if (job.status == 'Done' and
               job_age.days >= self.clean_jobs_after_days):
               job.remove()
               removed_jobs += 1
         logging.info('Autoclean removed %d jobs older than %d days' % 
            (removed_jobs, self.clean_jobs_after_days))
      except Exception as e:
         logging.exception('Autoclean encountered an error, exits cleanup')



