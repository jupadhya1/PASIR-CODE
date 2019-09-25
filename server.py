from flask import Flask, render_template, request, send_file, abort
from flask_jsonrpc import JSONRPC
from flask_httpauth import HTTPBasicAuth
import socket
import ssl
import os
import json 
from werkzeug import secure_filename
import logging
import uuid
from functools import wraps
import datetime
from threading import Thread

import config
import store
from autosync import AutosyncThread
from autoclean import AutocleanThread
from pasir import PasirTicketClassification

logging.info('========================================================')
logging.info('IBM PASIR/Classr API')
logging.info('IBM Internal Use Only')
logging.info('(c) IBM Research 2016')
logging.info('========================================================')

app = Flask(__name__)
jsonrpc = JSONRPC(app, '/api', enable_web_browsable_api=True)
server_dir = os.path.dirname(os.path.realpath(__file__))
auth = HTTPBasicAuth()

@auth.get_password
def get_pw(username):
    if config.API_KEY != '':
       return config.API_KEY
    else:
       return None

@app.route('/',methods=['GET', 'POST'])
def index():
   return render_template('index.html')

@app.route('/classify', methods=['GET'])
@auth.login_required
def classifier_ui():
   if not config.ENABLE_CLASSIFICATION: raise Exception('Classification not permitted on this API')
   return render_template('classify.html')

@app.route('/resource-upload', methods=['POST', 'GET'])
@auth.login_required
def upload_resource():
   if request.method == 'GET':
      return render_template('upload.html', uid='')
   else:
      # Get the name of the uploaded file
      logging.debug(request.files)
      file = request.files['file']
      filename = secure_filename(file.filename)
      file.save(os.path.join(config.TEMP_PATH, filename))
      resource_type = request.form['resourceType']
      resource_title = request.form['resourceTitle']
      resource_uid = request.form['resourceUid']
      # create resource object
      if (filename.endswith('.tar.gz')):
         resource = store.Resource.add_from_targz(
            uid = resource_uid, # specified optionally
            resource_type = resource_type,
            title = resource_title,
            gz_path = os.path.join(config.TEMP_PATH, filename))
      else:
         resource = store.Resource.add_from_file(
            uid = resource_uid, # specified optionally
            resource_type = resource_type,
            title = resource_title,
            file_path = os.path.join(config.TEMP_PATH, filename))
      return render_template('upload.html', uid=resource.uid)

@jsonrpc.method('classifier.get_all')
@auth.login_required
def get_classifiers():
   classifiers = store.Classifier.get_all()
   return json.dumps(classifiers, cls=store.StoreJsonEncoder)

@jsonrpc.method('classifier.get')
@auth.login_required
def get_classifier(uid):
   classifier = store.Classifier.get(uid)
   if classifier == None: raise Exception('Classifier %s not found' % uid)
   return json.dumps(classifier, cls=store.StoreJsonEncoder)

@jsonrpc.method('classifier.delete')
@auth.login_required
def delete_classifier(uid):
   classifier = store.Classifier.get(uid)
   if classifier == None: raise Exception('Classifier %s not found' % uid)
   classifier.remove()
   return json.dumps('OK')

@jsonrpc.method('classifier.enable')
@auth.login_required
def enable_classifier(uid):
   classifier = store.Classifier.get(uid)
   if classifier == None: raise Exception('Classifier %s not found' % uid)
   classifier.set_enabled(True)
   return json.dumps('OK')

@jsonrpc.method('classifier.disable')
@auth.login_required
def disable_classifier(uid):
   classifier = store.Classifier.get(uid)
   if classifier == None: raise Exception('Classifier %s not found' % uid)
   classifier.set_enabled(False)
   return json.dumps('OK')

@app.route('/api/job.place', methods=['POST', 'GET'])
@auth.login_required
def classify_tickets():
   if not config.ENABLE_CLASSIFICATION: raise Exception('Classification not permitted on this API')
   if request.method == 'GET':
      return render_template('classify.html')
   else:
      # if not check_api_key(request): 
      #    abort(401)
      #    return
      # process file upload
      logging.debug(request.files)
      file = request.files['file']
      filename = secure_filename(file.filename)
      # process POST params
      classifier_uid = request.form['classifierUid']
      in_desc_col = request.form['inDescCol']
      in_res_col = request.form['inResCol']
      out_class_col = request.form['outClassCol']
      # check things
      if not store.Classifier.exists(classifier_uid):
         raise Exception('Unknown classifier uid')
      if not filename.lower().endswith('.csv'):
         raise Exception('Method only accepts CSV file')
      # get classifier and cerate job context
      classifier = store.Classifier.get(classifier_uid)
      job_context = store.JobContext.create(classifier_uid)
      # save input file
      in_csv = os.path.join(job_context.work_dir, filename)
      file.save(in_csv)
      # define output file name
      out_csv = os.path.join(job_context.work_dir, 'autolabeled.tickets.csv')
      # invoke classifier
      classifier.classify(job_context, 
         in_csv, 
         in_desc_col, 
         in_res_col, 
         out_csv, 
         out_class_col)
      return json.dumps(job_context.uid)

@app.route('/api/job.download/<uid>')
@auth.login_required
def get_classifier_result(uid):
   job_context = store.JobContext.get(uid)
   if job_context == None: raise Exception('Job %s not found' % uid)
   file_path = os.path.join(
      job_context.work_dir, 'autolabeled.tickets.csv')
   if os.path.exists(file_path):
      return send_file(file_path, 
         as_attachment=True,
         attachment_filename='autolabeled.tickets.csv')
   else:
      raise Exception('Output for job %s not found' % (uid))

@jsonrpc.method('job.get_status')
@auth.login_required
def get_classifier_progress(uid):
   job_context = store.JobContext.get(uid)
   if job_context == None: raise Exception('Job %s not found' % uid)
   status = job_context.status
   progress_percentage = job_context.progress_percentage
   progress_text = job_context.progress_text
   return json.dumps({
      'status': status, 
      'progress_percentage': progress_percentage,
      'progress_text': progress_text})

@jsonrpc.method('job.delete')
@auth.login_required
def delete_job(uid):
   job_context = store.JobContext.get(uid)
   if job_context == None: raise Exception('Job %s not found' % uid)
   job_context.remove()
   return json.dumps('OK')

@jsonrpc.method('job.get_all')
@auth.login_required
def get_jobs():
   job_contexts = store.JobContext.get_all()
   return json.dumps(job_contexts, cls=store.StoreJsonEncoder)

@jsonrpc.method('resource.get_all')
@auth.login_required
def get_resources():
   resources = store.Resource.get_all()
   return json.dumps(resources, cls=store.StoreJsonEncoder)

@jsonrpc.method('resource.get')
@auth.login_required
def get_resource(uid):
   resource = store.Resource.get(uid)
   if resource == None: raise Exception('Resource %s not found' % uid)
   return json.dumps(resource, cls=store.StoreJsonEncoder)

@jsonrpc.method('resource.delete')
@auth.login_required
def delete_resource(uid):
   resource = store.Resource.get(uid)
   if resource == None: raise Exception('Resource %s not found' % uid)
   resource.remove()
   return json.dumps('OK')

@app.route('/api/resource.download/<uid>')
@auth.login_required
def download_resource(uid):
   if not config.ENABLE_RESOURCE_DOWNLOAD: raise Exception('Resource download not permitted on this API')
   resource = store.Resource.get(uid)
   if resource == None: raise Exception('Resource %s not found' % uid)
   resource_targz_path = resource.to_targz()
   return send_file(resource_targz_path)

@jsonrpc.method('pasir.classify')
#@auth.login_required
def pasir_classify(ticket_client_id, ticket_data_source, from_date, to_date):
   freshest_classifier = None
   classifiers = store.Classifier.get_all()
   for c in classifiers:
      if (c.enabled and c.title == 'PASIR' and c.trained):
         if freshest_classifier == None or c.finished_on > freshest_classifier.finished_on:
            freshest_classifier = c
   if freshest_classifier == None:
      raise Exception('No suitable PASIR classifier is available on this API')
   pasir_classification = PasirTicketClassification.create(
      classifier = freshest_classifier,
      client_id = ticket_client_id,
      data_source = ticket_data_source,
      from_timestamp = datetime.datetime.strptime(from_date,'%Y-%m-%d'), 
      to_timestamp = datetime.datetime.strptime(to_date,'%Y-%m-%d'))
   # launch job asynchronously and return immediately
   Thread(target = pasir_classification.fetch_and_classify).start()
   return json.dumps({
      'classr_ticketclassification_id' : pasir_classification.classr_ticketclassification_id,
      'job_id' : pasir_classification.job_context.uid
      })

# REST-like interface for classification
@app.route('/api/pasir.classify/<client_id>/<data_source>/<from_date>/<to_date>')
@auth.login_required
def pasir_classify_rest(client_id, data_source, from_date, to_date):
   return pasir_classify(client_id, data_source, from_date, to_date)

@app.route('/manual')
@auth.login_required
def pasir_compatibility():
   if not config.ENABLE_CLASSIFICATION: raise Exception('Classification not permitted on this API')
   return render_template('pasir-compatibility.html')

##############################################################

logging.info('Setting up server on %s' % socket.getfqdn())

# log API-ket configuration
if config.API_KEY == '':
   logging.info('No API-key authentication required')
else:
   logging.info('API-key: %s' % config.API_KEY)

# configure SSL as required
if (config.SSL_ENABLED and 
   config.SSL_PRIVATE_KEY != '' and 
   config.SSL_CERT != ''): 
   ssl_context = (config.SSL_CERT, config.SSL_PRIVATE_KEY)
else: ssl_context = None

# set up autosync threads
for name, autosync_config in config.AUTOSYNC.iteritems():
   autosync_thread = AutosyncThread(name, 
      autosync_config['URL'], 
      autosync_config['KEY'], 
      autosync_config['SYNC_MODEL_TYPES'], 
      autosync_config['SYNC_INTERVAL_HOURS']*3600)
   autosync_thread.start()

# set up autoelan thread
if config.CLEAN_JOBS_AFTER_DAYS > 0:
   autoclean_thread = AutocleanThread(
      config.CLEAN_JOBS_AFTER_DAYS)
   autoclean_thread.start()

# start HTTP listener
if __name__ == '__main__':
   app.run(host=config.IP_MASK, 
      debug=False, 
      threaded=True, 
      port=config.PORT, 
      ssl_context=ssl_context)


