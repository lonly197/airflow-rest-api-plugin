#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019-08-12 16:16
# @Author  : lonly
# @Site    : http://github.com/lonly197
# @File    : airflow_api.py
# @Software: PyCharm
# @Description: The Restful API For Airflow

import json
import logging
import os
import socket
import subprocess
import time
from datetime import datetime

import six
from airflow import configuration
from airflow import settings
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.models import DagBag, DagRun, DagModel, TaskInstance
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.dates import date_range as utils_date_range
from airflow.utils.state import State
from airflow.www.app import csrf
from flask import Blueprint, request, Response
from sqlalchemy import or_

airflow_api_blueprint = Blueprint('airflow_api', __name__, url_prefix='/api/v1')

# Getting configurations from airflow.cfg file
airflow_webserver_base_url = configuration.get('webserver', 'BASE_URL')
airflow_base_log_folder = configuration.get('core', 'BASE_LOG_FOLDER')
airflow_dags_folder = configuration.get('core', 'DAGS_FOLDER')

# Getting Versions and Global variables
hostname = socket.gethostname()


class ApiInputException(Exception):
    pass


class ApiResponse:

    def __init__(self):
        pass

    STATUS_OK = 200
    STATUS_BAD_REQUEST = 400
    STATUS_UNAUTHORIZED = 401
    STATUS_NOT_FOUND = 404
    STATUS_SERVER_ERROR = 500

    @staticmethod
    def standard_response(status, code=0, msg='', payload=''):
        json_data = json.dumps({
            'code': code,
            'msg': msg,
            'data': payload
        })
        resp = Response(json_data, status=status, mimetype='application/json')
        return resp

    @staticmethod
    def success(payload):
        return ApiResponse.standard_response(ApiResponse.STATUS_OK, code=0, msg='success', payload=payload)

    @staticmethod
    def error(status, error):
        return ApiResponse.standard_response(status, code=1, msg=error)

    @staticmethod
    def bad_request(error):
        return ApiResponse.error(ApiResponse.STATUS_BAD_REQUEST, error)

    @staticmethod
    def not_found(error='Resource not found'):
        return ApiResponse.error(ApiResponse.STATUS_NOT_FOUND, error)

    @staticmethod
    def unauthorized(error='Not authorized to access this resource'):
        return ApiResponse.error(ApiResponse.STATUS_UNAUTHORIZED, error)

    @staticmethod
    def server_error(error='An unexpected problem occurred'):
        return ApiResponse.error(ApiResponse.STATUS_SERVER_ERROR, error)


class ApiUtil:
    def __init__(self):
        pass

    @staticmethod
    def get_dag(dag_id):
        # check dag_id
        if dag_id is None:
            logging.warning("The dag_id argument wasn't provided")
            raise Exception("The dag_id argument should be provided")

        dagbag = DagBag('dags')

        if dag_id not in dagbag.dags:
            raise Exception("Dag id {} not found".format(dag_id))
        return dagbag.get_dag(dag_id)

    @staticmethod
    def format_dag_run(dag_run):
        return {
            'run_id': dag_run.run_id,
            'dag_id': dag_run.dag_id,
            'state': dag_run.get_state(),
            'start_date': (None if not dag_run.start_date else str(dag_run.start_date)),
            'end_date': (None if not dag_run.end_date else str(dag_run.end_date)),
            'external_trigger': dag_run.external_trigger,
            'execution_date': str(dag_run.execution_date)
        }

    @staticmethod
    def find_dag_runs(session, dag_id, dag_run_id, execution_date):
        qry = session.query(DagRun)
        qry = qry.filter(DagRun.dag_id == dag_id)
        qry = qry.filter(or_(DagRun.run_id == dag_run_id, DagRun.execution_date == execution_date))
        return qry.order_by(DagRun.execution_date).all()

    @staticmethod
    def format_task_instance(task):
        return {
            'dag_id': task.dag_id,
            'task_id': task.task_id,
            'job_id': task.job_id,
            'state': task.state,
            'start_date': (None if not task.start_date else str(task.start_date)),
            'end_date': (None if not task.end_date else str(task.end_date)),
            'execution_date': (None if not task.execution_date else str(task.execution_date)),
            'queued_dttm': (None if not task.queued_dttm else str(task.queued_dttm)),
            'hostname': task.hostname,
            'unixname': task.unixname,
            'queue': task.queue,
            'priority_weight': task.priority_weight,
            'try_number': task.try_number,
            'pid': task.pid
        }

    @staticmethod
    def check_dag_active(dag_id):
        dagbag = DagBag('dags')
        dag = dagbag.get_dag(dag_id)
        return not dag.is_paused

    @staticmethod
    def pause_dag(dag_id):
        airflow_cmd_split = ["airflow", "pause", dag_id]
        cli_output = ApiUtil.execute_cli_command(airflow_cmd_split)
        logging.info("Pause Dag Result: " + str(cli_output))

    @staticmethod
    def remove_dag(dag_id):
        logging.info("Executing custom 'remove_dag' function")
        dagbag = DagBag('dags')
        dag = dagbag.get_dag(dag_id)
        # Get Dag File Path
        dag_path = dag.full_filepath if dag is not None else os.path.join(airflow_dags_folder, dag_id + ".py")
        rm_dag_cmd_split = ["rm", "-rf", dag_path]
        cli_output = ApiUtil.execute_cli_command(rm_dag_cmd_split)
        logging.info("Remove Dag File[{}] Result: {}".format(dag.full_filepath, str(cli_output)))

        dag_cache_path = os.path.join(airflow_dags_folder, '__pycache__', dag_id + ".cpython-36.pyc")
        rm_dag_cache_cmd_split = ["rm", "-rf", dag_cache_path]
        cli_output = ApiUtil.execute_cli_command(rm_dag_cache_cmd_split)
        logging.info("Remove Dag Cache File[{}] Result: {}".format(dag_cache_path, str(cli_output)))

    @staticmethod
    def execute_cli_command_background_mode(airflow_cmd):
        logging.info("Executing CLI Command in the Background: {}".format(str(airflow_cmd)))
        exit_code = os.system(airflow_cmd)
        output = ApiUtil.get_empty_process_output()
        output["stdout"] = "exit_code: " + str(exit_code)
        return output

    # General execution of the airflow command passed to it and returns the response
    @staticmethod
    def execute_cli_command(airflow_cmd_split):
        logging.info("Executing CLI Command: {}".format(str(airflow_cmd_split)))
        process = subprocess.Popen(airflow_cmd_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        return ApiUtil.collect_process_output(process)

    # gets and empty object that has all the fields a CLI function would have in it.
    @staticmethod
    def get_empty_process_output():
        return {
            "stderr": "",
            "stdin": "",
            "stdout": ""
        }

    # Get the output of the CLI process and package it in a dict
    @staticmethod
    def collect_process_output(process):
        output = ApiUtil.get_empty_process_output()
        if process.stderr is not None:
            output["stderr"] = ""
            for line in process.stderr.readlines():
                output["stderr"] += str(line)
        if process.stdin is not None:
            output["stdin"] = ""
            for line in process.stdin.readlines():
                output["stdin"] += str(line)
        if process.stdout is not None:
            output["stdout"] = ""
            for line in process.stdout.readlines():
                output["stdout"] += str(line)
        logging.info("RestAPI Output: " + str(output))
        return output

    # Checks a string object to see if it is none or empty so we can determine if an argument (passed to the rest api) is provided
    @staticmethod
    def is_arg_not_provided(arg):
        return arg is None or arg == ""


@airflow_api_blueprint.before_request
def verify_authentication():
    """
    Verify Authentication
    """
    authorization = request.headers.get('authorization')
    try:
        api_auth_key = settings.conf.get('AIRFLOW_API_PLUGIN', 'AIRFLOW_API_AUTH')
    except AirflowConfigException:
        return

    if authorization != api_auth_key:
        return ApiResponse.unauthorized("You are not authorized to use this resource")


@csrf.exempt
@airflow_api_blueprint.route('/dag/<dag_id>', methods=['GET'])
def get_dag(dag_id):
    """
    Gets the DAG Info By DagID
    """
    logging.info("Executing custom 'get_dag' function")

    # check dag_id argument
    if dag_id is None:
        logging.warning("The dag_id argument wasn't provided")
        return ApiResponse.bad_request("The dag_id argument should be provided")

    payload = {
        'dag_id': dag_id,
        'full_path': None,
        'is_active': False,
        'last_execution': None,
    }
    dagbag = DagBag('dags')
    if dag_id not in dagbag.dags:
        return ApiResponse.bad_request("Dag id {} not found".format(dag_id))
    dag = dagbag.get_dag(dag_id)
    if dag:
        payload['full_path'] = dag.full_filepath
        payload['is_active'] = (not dag.is_paused)
        payload['last_execution'] = str(dag.latest_execution_date)
    return ApiResponse.success(payload)


@csrf.exempt
@airflow_api_blueprint.route('/dag/check/<dag_id>', methods=['GET'])
def check_dag_is_active(dag_id):
    """
    Check the DAG is active or not by DagID
    """
    logging.info("Executing custom 'check_dag_is_active' function")

    # check dag_id
    if dag_id is None:
        logging.warning("The dag_id argument wasn't provided")
        return ApiResponse.bad_request("The dag_id argument should be provided")

    dagbag = DagBag('dags')

    if dag_id not in dagbag.dags:
        return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

    dag = dagbag.get_dag(dag_id)
    payload = not dag.is_paused
    return ApiResponse.success(payload)


@csrf.exempt
@airflow_api_blueprint.route('/dag/exist/<dag_id>', methods=['GET'])
def check_dag_file_exist(dag_id):
    logging.info("Executing custom 'check_dag_file_exist' function")

    # check dag_id
    if dag_id is None:
        logging.warning("The dag_id argument wasn't provided")
        return ApiResponse.bad_request("The dag_id argument should be provided")

    dagbag = DagBag('dags')
    if dag_id not in dagbag.dags:
        return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

    dag = dagbag.get_dag(dag_id)
    payload = os.path.exists(dag.full_filepath)
    return ApiResponse.success(payload)


@csrf.exempt
@airflow_api_blueprint.route('/dags', methods=['GET'])
def list_dags():
    logging.info("Executing custom 'list_dags' function")
    dagbag = DagBag('dags')
    dags = []
    for dag_id in dagbag.dags:
        payload = {
            'dag_id': dag_id,
            'full_path': None,
            'is_active': False,
            'last_execution': None,
        }

        dag = dagbag.get_dag(dag_id)

        if dag:
            payload['full_path'] = dag.full_filepath
            payload['is_active'] = (not dag.is_paused)
            payload['last_execution'] = str(dag.latest_execution_date)

        dags.append(payload)

    return ApiResponse.success(dags)


@csrf.exempt
@airflow_api_blueprint.route('/dag_stats', methods=['GET'])
def get_dag_stats():
    logging.info("Executing custom 'get_dag_stats' function")
    try:
        # Recall Airflow Delete URL
        from airflow.www.views import Airflow
        return Airflow().dag_stats()
    except Exception as e:
        error_message = "An error occurred while trying to dag_stats'" + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)


@csrf.exempt
@airflow_api_blueprint.route('/dag_runs', methods=['GET'])
def get_dag_runs():
    logging.info("Executing custom 'get_dag_runs' function")
    dag_runs = []

    session = settings.Session()
    query = session.query(DagRun)

    if request.args.get('state') is not None:
        query = query.filter(DagRun.state == request.args.get('state'))

    if request.args.get('external_trigger') is not None:
        # query = query.filter(DagRun.external_trigger == (request.args.get('external_trigger') is True))
        query = query.filter(DagRun.external_trigger == (request.args.get('external_trigger') in ['true', 'True']))

    if request.args.get('dag_id') is not None:
        query = query.filter(DagRun.dag_id == request.args.get('dag_id'))

    if request.args.get('run_id') is not None:
        query = query.filter(DagRun.run_id == request.args.get('run_id'))

    if request.args.get('prefix') is not None:
        query = query.filter(DagRun.run_id.ilike('{}%'.format(request.args.get('prefix'))))

    runs = query.order_by(DagRun.execution_date).all()

    for run in runs:
        dag_runs.append(ApiUtil.format_dag_run(run))

    session.close()

    return ApiResponse.success(dag_runs)


@csrf.exempt
@airflow_api_blueprint.route('/dag_runs', methods=['POST'])
def create_dag_run():
    logging.info("Executing custom 'create_dag_run' function")
    # decode input
    data = request.get_json(force=True)
    # ensure there is a dag id
    if 'dag_id' not in data or data['dag_id'] is None:
        return ApiResponse.bad_request('Must specify the dag id to create dag runs for')
    dag_id = data['dag_id']

    limit = 500
    partial = False
    if 'limit' in data and data['limit'] is not None:
        try:
            limit = int(data['limit'])
            if limit <= 0:
                return ApiResponse.bad_request('Limit must be a number greater than 0')
            if limit > 500:
                return ApiResponse.bad_request('Limit cannot exceed 500')
        except ValueError:
            return ApiResponse.bad_request('Limit must be an integer')

    if 'partial' in data and data['partial'] in ['true', 'True', True]:
        partial = True

    # ensure there is run data
    start_date = datetime.now()
    end_date = datetime.now()

    if 'start_date' in data and data['start_date'] is not None:
        try:
            start_date = datetime.strptime(data['start_date'], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            error = '\'start_date\' has invalid format \'{}\', Ex format: YYYY-MM-DDThh:mm:ss'
            return ApiResponse.bad_request(error.format(data['start_date']))

    if 'end_date' in data and data['end_date'] is not None:
        try:
            end_date = datetime.strptime(data['end_date'], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            error = '\'end_date\' has invalid format \'{}\', Ex format: YYYY-MM-DDThh:mm:ss'
            return ApiResponse.bad_request(error.format(data['end_date']))

    # determine run_id prefix
    prefix = 'manual_{}'.format(int(time.time()))
    if 'prefix' in data and data['prefix'] is not None:
        prefix = data['prefix']

        if 'backfill' in prefix:
            return ApiResponse.bad_request('Prefix cannot contain \'backfill\', Airflow will ignore dag runs using it')
        # ensure prefix doesn't have an underscore appended
        if prefix[:-1:] == "_":
            prefix = prefix[:-1]

    conf = None
    if 'conf' in data and data['conf'] is not None:
        if isinstance(data['conf'], six.string_types):
            conf = data['conf']
        else:
            try:
                conf = json.dumps(data['conf'])
            except Exception:
                return ApiResponse.bad_request('Could not encode specified conf JSON')

    try:
        session = settings.Session()
        dagbag = DagBag('dags')
        if dag_id not in dagbag.dags:
            return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

        dag = dagbag.get_dag(dag_id)
        # ensure run data has all required attributes and that everything is valid, returns transformed data
        runs = utils_date_range(start_date=start_date, end_date=end_date, delta=dag._schedule_interval)

        if len(runs) > limit and partial is False:
            error = '{} dag runs would be created, which exceeds the limit of {}.' \
                    ' Reduce start/end date to reduce the dag run count'
            return ApiResponse.bad_request(error.format(len(runs), limit))

        payloads = []
        for exec_date in runs:
            run_id = '{}_{}'.format(prefix, exec_date.isoformat())
            if ApiUtil.find_dag_runs(session, dag_id, run_id, exec_date):
                continue
            payloads.append({
                'run_id': run_id,
                'execution_date': exec_date,
                'conf': conf
            })
        results = []
        for index, run in enumerate(payloads):
            if len(results) >= limit:
                break
            dag.create_dagrun(
                run_id=run['run_id'],
                execution_date=run['execution_date'],
                state=State.RUNNING,
                conf=conf,
                external_trigger=True
            )
            results.append(run['run_id'])

        session.close()
    except ApiInputException as e:
        return ApiResponse.bad_request(str(e))
    except ValueError as e:
        return ApiResponse.server_error(str(e))
    except AirflowException as e:
        return ApiResponse.server_error(str(e))
    except Exception as e:
        return ApiResponse.server_error(str(e))

    return ApiResponse.success({'dag_run_ids': results})


@csrf.exempt
@airflow_api_blueprint.route('/dag_runs/<dag_run_id>', methods=['GET'])
def get_dag_run(dag_run_id):
    logging.info("Executing custom 'get_dag_run' function")
    session = settings.Session()
    runs = DagRun.find(run_id=dag_run_id, session=session)
    if len(runs) == 0:
        return ApiResponse.not_found('Dag run not found')
    dag_run = runs[0]
    session.close()
    return ApiResponse.success(ApiUtil.format_dag_run(dag_run))


# Custom Function for the upload_dag API
@csrf.exempt
@airflow_api_blueprint.route('/dag/upload', methods=['POST'])
def upload_dag():
    logging.info("Executing custom 'upload_dag' function")

    # check if the post request has the file part
    if 'dag_file' not in request.files or request.files['dag_file'].filename == '':
        logging.warning("The dag_file argument wasn't provided")
        return ApiResponse.bad_request("dag_file should be provided")
    dag_file = request.files['dag_file']

    force = True if request.form.get('force') is not None else False
    logging.info("upload_dag force upload: " + str(force))

    pause = True if request.form.get('pause') is not None else False
    logging.info("upload_dag in pause state: " + str(pause))

    unpause = True if request.form.get('unpause') is not None else False
    logging.info("upload_dag in unpause state: " + str(unpause))

    # make sure that the dag_file is a python script
    if dag_file and dag_file.filename.endswith(".py"):
        save_file_path = os.path.join(airflow_dags_folder, dag_file.filename)

        # Check if the file already exists.
        if os.path.isfile(save_file_path) and not force:
            logging.warning("File to upload already exists")
            return ApiResponse.bad_request(
                "The file '" + save_file_path + "' already exists on host '" + hostname + "'.")

        logging.info("Saving file to '" + save_file_path + "'")
        dag_file.save(save_file_path)

    else:
        logging.warning("The upload_dag file is not a python file. It does not end with a .py.")
        return ApiResponse.bad_request("The dag_file is not a *.py file")

    # if both the pause and unpause options are provided then skip the pausing and unpausing phase
    if not (pause and unpause):
        if pause or unpause:
            try:
                # import the DAG file that was uploaded so that we can get the DAG_ID to execute the command to pause or unpause it
                import importlib
                dag_file = importlib.load_source('module.name', save_file_path)
                dag_id = dag_file.dag.dag_id

                # run the pause or unpause cli command
                airflow_cmd_split = []
                if pause:
                    airflow_cmd_split = ["airflow", "pause", dag_id]
                if unpause:
                    airflow_cmd_split = ["airflow", "unpause", dag_id]
                cli_output = ApiUtil.execute_cli_command(airflow_cmd_split)
                logging.info("Execute Pause Or Unpause Result: " + str(cli_output))
            except Exception as e:
                warning = "Failed to set the state (pause, unpause) of the DAG: " + str(e)
                logging.warning(warning)
    else:
        warning = "Both options pause and unpause were given. Skipping setting the state (pause, unpause) of the DAG."
        logging.warning(warning)
    return ApiResponse.success("DAG File [{}] has been uploaded".format(dag_file))


# Custom Function for the deploy_dag API
@csrf.exempt
@airflow_api_blueprint.route('/dag/deploy', methods=['POST'])
def deploy_dag():
    logging.info("Executing custom 'deploy_dag' function")

    dag_content = request.form.get('dag_content')
    dag_name = request.form.get('dag_name')

    # check if the post request has the file part
    if ApiUtil.is_arg_not_provided(dag_content) or ApiUtil.is_arg_not_provided(dag_name):
        logging.warning("The dag_content and dag_name argument wasn't provided")
        return ApiResponse.bad_request("dag_content and dag_name should be provided")

    force = True if request.form.get('force') is not None else False
    logging.info("deploy_dag force upload: " + str(force))

    pause = True if request.form.get('pause') is not None else False
    logging.info("deploy_dag in pause state: " + str(pause))

    unpause = True if request.form.get('unpause') is not None else False
    logging.info("deploy_dag in unpause state: " + str(unpause))

    save_file_path = os.path.join(airflow_dags_folder, dag_name + '.py')

    # Check if the file already exists.
    if os.path.isfile(save_file_path) and not force:
        logging.warning("File {} already exists".format(save_file_path))
        return ApiResponse.bad_request(
            "The file '" + save_file_path + "' already exists on host '" + hostname + "'.")

    logging.info("Saving file to '" + save_file_path + "'")
    with open(save_file_path, "w") as text_file:
        text_file.write(dag_content)

    # if both the pause and unpause options are provided then skip the pausing and unpausing phase
    if not (pause and unpause):
        if pause or unpause:
            try:
                # import the DAG file that was uploaded so that we can get the DAG_ID to execute the command to pause or unpause it
                import importlib
                dag_file = importlib.load_source('module.name', save_file_path)
                dag_id = dag_file.dag.dag_id

                # run the pause or unpause cli command
                airflow_cmd_split = []
                if pause:
                    airflow_cmd_split = ["airflow", "pause", dag_id]
                if unpause:
                    airflow_cmd_split = ["airflow", "unpause", dag_id]
                cli_output = ApiUtil.execute_cli_command(airflow_cmd_split)
                logging.info("Execute Pause Or Unpause Result: " + str(cli_output))
            except Exception as e:
                warning = "Failed to set the state (pause, unpause) of the DAG: " + str(e)
                logging.warning(warning)
    else:
        warning = "Both options pause and unpause were given. Skipping setting the state (pause, unpause) of the DAG."
        logging.warning(warning)
    return ApiResponse.success("DAG File [{}] has been deployed".format(dag_name))


# Custom Function for the refresh_dag API
# This will call the direct function corresponding to the web endpoint '/admin/airflow/refresh' that already exists in Airflow
@csrf.exempt
@airflow_api_blueprint.route('/dag/refresh/<dag_id>', methods=['GET'])
def refresh_dag(dag_id):
    logging.info("Executing custom 'refresh_dag' function")

    # Check dag_id argument
    if dag_id is None:
        logging.warning("The dag_id argument wasn't provided")
        return ApiResponse.bad_request("The dag_id argument should be provided")
    if dag_id not in DagBag('dags').dags:
        return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

    session = None
    try:
        # TODO: Is this method still needed after AIRFLOW-3561?
        session = settings.Session()
        dm = DagModel
        orm_dag = session.query(dm).filter(dm.dag_id == dag_id).first()

        if orm_dag:
            orm_dag.last_expired = time.timezone.utcnow()
            session.merge(orm_dag)
        session.commit()
        session.close()
        logging.info("Refresh Dag Result: {}".format(str(refresh_dag)))
    except Exception as e:
        error_message = "An error occurred while trying to Refresh the DAG '" + str(dag_id) + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)
    finally:
        if session is not None:
            session.close()
    return ApiResponse.success("DAG [{}] is now fresh as a daisy".format(dag_id))


@csrf.exempt
@airflow_api_blueprint.route('/dag/delete/<dag_id>', methods=['DELETE'])
def delete_dag(dag_id):
    logging.info("Executing custom 'delete_dag' function")

    # Check dag_id argument
    if dag_id is None:
        logging.warning("The dag_id argument wasn't provided")
        return ApiResponse.bad_request("The dag_id argument should be provided")
    if dag_id not in DagBag('dags').dags:
        return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

    try:
        # Pause Dag
        ApiUtil.pause_dag(dag_id)
        # Remove Dag File
        ApiUtil.remove_dag(dag_id)
        # Recall Airflow Delete URL
        from airflow.api.common.experimental import delete_dag
        delete_dag.delete_dag(dag_id)
        logging.info("Delete Result Success.")
    except Exception as e:
        error_message = "An error occurred while trying to Delete the DAG '" + str(dag_id) + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)
    return ApiResponse.success("DAG [{}] has been deleted".format(dag_id))


@csrf.exempt
@airflow_api_blueprint.route('/dag/pause/<dag_id>', methods=['GET'])
def pause_dag(dag_id):
    logging.info("Executing custom 'pause_dag' function")

    try:
        ApiUtil.pause_dag(dag_id)
    except Exception as e:
        error_message = "An error occurred while trying to pause the DAG '" + str(dag_id) + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)
    return ApiResponse.success("DAG [{}] has been paused".format(dag_id))


@csrf.exempt
@airflow_api_blueprint.route('/dag/unpause/<dag_id>', methods=['GET'])
def unpause_dag(dag_id):
    logging.info("Executing custom 'unpause_dag' function")
    try:
        airflow_cmd_split = ["airflow", "unpause", dag_id]
        cli_output = ApiUtil.execute_cli_command(airflow_cmd_split)
        logging.info("UnPause Dag Result: " + str(cli_output))
    except Exception as e:
        error_message = "An error occurred while trying to unpause the DAG '" + str(dag_id) + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)
    return ApiResponse.success("DAG [{}] has been unpaused".format(dag_id))


@csrf.exempt
@airflow_api_blueprint.route('/dag/trigger/<dag_id>', methods=['GET'])
def trigger_dag(dag_id):
    logging.info("Executing custom 'trigger_dag' function")

    # Check dag_id argument
    if dag_id is None:
        logging.warning("The dag_id argument wasn't provided")
        return ApiResponse.bad_request("The dag_id argument should be provided")
    dagbag = DagBag('dags')
    if dag_id not in DagBag('dags').dags:
        return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

    try:
        # Get Dag From DagBag
        dag = dagbag.get_dag(dag_id)
        # Check Dag Status
        if dag.is_paused:
            logging.warning("The Dag[{}] is not active".format(dag_id))
            # UnPause Dag
            airflow_cmd_split = ["airflow", "unpause", dag_id]
            cli_output = ApiUtil.execute_cli_command(airflow_cmd_split)
            logging.info("UnPause Dag Result: " + str(cli_output))
            # Check Dag Status again
            if not ApiUtil.check_dag_active(dag_id):
                raise Exception("Dag is still not active")

                # Trigger Dag By Dag ID
        airflow_cmd_split = ["airflow", "trigger_dag"]

        run_id = request.args.get('run_id')
        if run_id is not None:
            logging.info("trigger dag run_id: " + str(run_id))
            airflow_cmd_split.extend(["-r", run_id])

        execution_date = request.args.get('execution_date')
        if execution_date is not None:
            logging.info("trigger dag execution_date: " + str(execution_date))
            airflow_cmd_split.extend(["-e", execution_date])

        airflow_cmd_split.append(dag_id)
        cli_output = ApiUtil.execute_cli_command(airflow_cmd_split)
        logging.info("Trigger Dag Result: " + str(cli_output))

    except Exception as e:
        error_message = "An error occurred while trying to trigger the DAG '" + str(dag_id) + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)
    return ApiResponse.success("DAG [{}] has been triggered".format(dag_id))


@csrf.exempt
@airflow_api_blueprint.route('/dag/log/<dag_id>', methods=['GET'])
def get_dag_log(dag_id):
    logging.info("Executing custom 'get_dag_log' function")

    # check dag_id
    if dag_id is None:
        logging.warning("The dag_id argument wasn't provided")
        return ApiResponse.bad_request("The dag_id argument should be provided")

    if dag_id not in DagBag('dags').dags:
        return ApiResponse.bad_request("Dag id {} not found".format(dag_id))

    session = None
    try:
        session = settings.Session()
        tasks = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()

        session.close()
        dag_tasks = []

        for task in tasks:
            dag_tasks.append(ApiUtil.format_task_instance(task))

        return ApiResponse.success(dag_tasks)
    except Exception as e:
        error_message = "An error occurred while trying to get_dag_log of DAG '" + str(dag_id) + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)
    finally:
        if session is not None:
            session.close()


@csrf.exempt
@airflow_api_blueprint.route('/task_stats', methods=['GET'])
def get_task_stats():
    logging.info("Executing custom 'get_task_stats' function")
    try:
        # Recall Airflow Delete URL
        from airflow.www.views import Airflow
        return Airflow().task_stats()
    except Exception as e:
        error_message = "An error occurred while trying to task_stats'" + "': " + str(e)
        logging.error(error_message)
        return ApiResponse.server_error(error_message)


# Creating the AirflowRestApiPlugin which extends the AirflowPlugin so its imported into Airflow
class AirflowRestApiPlugin(AirflowPlugin):
    name = "rest_api"
    operators = []
    flask_blueprints = [airflow_api_blueprint]
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
