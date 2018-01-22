import errno
import logging
import os
import subprocess

def get_logger():
  return logging.getLogger(__name__)

def create_fn_api_runner():
  from apache_beam.runners.portability.fn_api_runner import FnApiRunner # pylint: disable=E0401
  return FnApiRunner()

def get_cloud_project():
  cmd = [
    'gcloud', '-q', 'config', 'list', 'project',
    '--format=value(core.project)'
  ]
  with open(os.devnull, 'w') as dev_null:
    try:
      res = subprocess.check_output(cmd, stderr=dev_null).strip()
      if not res:
        raise Exception(
          '--cloud specified but no Google Cloud Platform '
          'project found.\n'
          'Please specify your project name with the --project '
          'flag or set a default project: '
          'gcloud config set project YOUR_PROJECT_NAME'
        )
      return res
    except OSError as e:
      if e.errno == errno.ENOENT:
        raise Exception(
          'gcloud is not installed. The Google Cloud SDK is '
          'necessary to communicate with the Cloud ML service. '
          'Please install and set up gcloud.'
        )
      raise

def get_default_job_name(name, suffix=''):
  from getpass import getuser
  from time import gmtime, strftime
  timestamp_str = strftime("%Y%m%d-%H%M%S", gmtime())
  return '%s-%s%s-%s' % (name or 'beamapp', getuser(), suffix or '', timestamp_str)

def add_cloud_args(parser):
  parser.add_argument(
    '--cloud',
    default=False,
    action='store_true'
  )
  parser.add_argument(
    '--runner',
    required=False,
    default=None,
    help='Runner.'
  )
  parser.add_argument(
    '--project',
    type=str,
    help='The cloud project name to be used for running this pipeline'
  )
  parser.add_argument(
    '--num_workers',
    default=1,
    type=int,
    help='The number of workers.'
  )
  parser.add_argument(
    '--job_name', type=str, required=False,
    help='The name of the cloud job'
  )
  parser.add_argument(
    '--job-name-suffix', type=str, required=False,
    help='A suffix appended to the job name'
  )

def process_cloud_args(parsed_args, output_path, name=None):
  if parsed_args.num_workers:
    parsed_args.autoscaling_algorithm = 'NONE'
    parsed_args.max_num_workers = parsed_args.num_workers
  parsed_args.setup_file = './setup.py'

  if parsed_args.cloud:
    # Flags which need to be set for cloud runs.
    default_values = {
      'project':
        get_cloud_project(),
      'temp_location':
        os.path.join(os.path.dirname(output_path), 'temp'),
      'runner':
        'DataflowRunner',
      'save_main_session':
        True,
    }
    if not parsed_args.job_name:
      parsed_args.job_name = get_default_job_name(name, parsed_args.job_name_suffix)
  else:
    # Flags which need to be set for local runs.
    default_values = {
      'runner': 'DirectRunner',
    }

  get_logger().info('default_values: %s', default_values)
  for kk, vv in default_values.iteritems():
    if kk not in parsed_args or not vars(parsed_args)[kk]:
      vars(parsed_args)[kk] = vv

  if parsed_args.runner == 'FnApiRunner':
    parsed_args.runner = create_fn_api_runner()
