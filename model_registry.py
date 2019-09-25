import os
import time

import xgbm.trainer
import xgbm.classifier

def train(job_context, 
          model_type, 
          meta, 
          resources, 
          in_csv, 
          in_desc_col, 
          in_res_col, 
          in_class_col): None

def classify(job_context, 
             model_type, 
             meta, 
             resources, 
             in_csv, 
             in_desc_col, 
             in_res_col,
             out_csv,
             out_class_col):
   """
   Use this function to hook up classifier algorithms to perform classification jobs. 
   This function receives all parameters you need to know about how the classification 
   should be performed.

   Use the model_type parameter to switch which algorithm to call.

     * in_csv       : full path of the input file that contains the records to be
                      classified. Params in_desc_col and in_res_col define the input CSV
                      column names for description and resolution, respectively.

     * out_csv      : full path of the output file to be created. If your classifier
                      returns with no exception, this file is expected to exist.
                      The output CSV must contain the class labels in a column
                      named as specified in out_class_col.

     * meta         : this is a dict containing configuration specific to
                      your model_type. Use this module's train() function to set up
                      the contents of this dict when creating/training the classifier

     * resources    : dict containing file system paths of static resources specific 
                      to your model_type. Use this module's train() function to set 
                      up the content of this dict when creating/training the classifier

     * job_context.work_dir 
                    : returns the full path of a dedicated work directory,
                      where your classifier is free to create any files,
                      including out_csv itself. Do not create files elsewhere.

     * job_context.logger
                    : use this Python Logger object to perform any logging
                      from your algorithm, e.g. job.logger.info('Doing something')
                      The log is located under <job.work_dir>/progress.log

     * job_context.update_progress(percentage, text)
                    : call this function to inform users of the API about the
                      progress of your classification job, e.g.
                      job_context.update_progress(5, 'Verifying input')

   """
   try:
      if model_type == 'XGBM':
         # parametrize call for the XGBM (pv+bow) classifier
         xgbm.classifier.run(
            in_csv                 = in_csv,
            in_voc                 = os.path.join(resources['vocab'], 'vocab.txt'),
            out_csv                = out_csv,
            in_model               = os.path.join(resources['model'], 'model.dat'),
            pv_weights_dir         = resources['pv'],
            vec_desc_dm            = os.path.join(job_context.work_dir, 'vec-desc-dm.csv'),
            vec_desc_dbow          = os.path.join(job_context.work_dir, 'vec-desc-dbow.csv'),
            vec_res_dm             = os.path.join(job_context.work_dir, 'vec-res-dm.csv'),
            vec_res_dbow           = os.path.join(job_context.work_dir, 'vec-res-dbow.csv'),
            pv_desc_txt            = os.path.join(job_context.work_dir, 'desc.txt'),
            pv_res_txt             = os.path.join(job_context.work_dir, 'res.txt'),
            desc_col               = in_desc_col,
            res_col                = in_res_col,
            clean_desc_out_col     = 'clean.description',
            clean_res_out_col      = 'clean.resolution',
            clean_combined_out_col = 'clean.combined',
            ticketclass_out_col    = out_class_col,
            dm_dim                 = 200,
            dm_objective           = 'negative',
            dm_negative_samples    = 10,
            dm_window              = 4,
            dm_subsample           = 0.1,
            dm_iters               = 30,
            dbow_dim               = 200,
            dbow_objective         = 'negative',
            dbow_negative_samples  = 5,
            dbow_window            = 6,
            dbow_subsample         = 0,
            dbow_iters             = 30,
            threads                = 4,
            max_ngram              = 1,
            orig_input             = '',
            job_context            = job_context)
      else:
         raise Exception('Model type %s is not supported by this server' % model_type)
      job_context.mark_done()
      return
   except Exception as e:
      job_context.update_progress(100, str(e), 'Error')

