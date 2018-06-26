import argparse
import dalmatian
import pandas as pd
import json
import os
import numpy as np
import sys
from google.cloud import storage
import csv
from agutil.parallel import parallelize, parallelize2
from agutil import status_bar, byteSize
from hashlib import md5
import time
import tempfile
import subprocess
from itertools import repeat
import re
from io import StringIO
import yaml

data_path = os.path.join(
    os.path.expanduser('~'),
    '.lapdog'
)

def load_data():
    try:
        with open(data_path) as reader:
            return json.load(reader)
    except FileNotFoundError:
        return {}

def workspaceType(text):
    """
    Text should either be a lapdog workspace name (no '/')
    or a firecloud namespace/workspace path
    """
    data = load_data()

    if 'workspaces' not in data:
        data['workspaces'] = {}

    if '/' not in text:
        if text not in data['workspaces']:
            sys.exit("Workspace '%s' is not recognized" % text)

        return dalmatian.WorkspaceManager(
            data['workspaces'][text]['namespace'],
            data['workspaces'][text]['workspace']
        )
    else:
        namespace,workspace = text.split('/')
        return dalmatian.WorkspaceManager(
            namespace, workspace
        )

@parallelize2()
def upload(bucket, path, source):
    blob = bucket.blob(path)
    # print("Commencing upload:", source)
    blob.upload_from_filename(source)

@parallelize2()
def download(blob, local_path):
    blob.download_to_filename(local_path)

def gs_copy(source, dest, project=None, user_project=None, move=False):
    import shutil
    from google.cloud import storage
    import os
    def fetch_blob(path):
        components = path[5:].split('/')
        bucket = storage.Client(
            project=project
        ).bucket(
            components[0],
            user_project=user_project
        )
        if not bucket.exists():
            bucket.create(project=project)
        return bucket.blob(
            '/'.join(components[1:])
        )
    source_local = not source.startswith('gs://')
    dest_local = not source.startswith('gs://')
    if source_local and dest_local:
        shutil.copyfile(source, dest)
    elif not (source_local or dest_local):
        source = fetch_blob(source)
        dest = fetch_blob(dest)
        dest.rewrite(source)
    elif source_local:
        fetch_blob(dest).upload_from_filename(
            source
        )
    else:
        source = fetch_blob(source)
        source.download_from_filename(
            dest
        )
    if move:
        if source_local:
            os.remove(source)
        else:
            source.delete()


def getblob(gs_path):
    bucket_id = gs_path[5:].split('/')[0]
    bucket_path = '/'.join(gs_path[5:].split('/')[1:])
    return storage.Blob(
        bucket_path,
        storage.Client().get_bucket(bucket_id)
    )

def main():
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        'workspace',
        type=workspaceType,
        help="Lapdog workspace alias (see workspace subcommand)\n"
        "Or Firecloud workspace in 'namespace/workspace' format",
    )

    parser = argparse.ArgumentParser(
        'lapdog',
        description="Command line interface to dalmatian and firecloud"
    )
    subparsers = parser.add_subparsers(metavar='<subcommand>')

    ws_parser = subparsers.add_parser(
        'workspace',
        help="Registers a workspace with lapdog",
        description="Registers a workspace with lapdog"
    )
    ws_parser.set_defaults(func=cmd_add_workspace)
    ws_parser.add_argument(
        'namespace',
        help="Namespace the workspace is under"
    )
    ws_parser.add_argument(
        'workspace',
        help="Name of the workspace"
    )
    ws_parser.add_argument(
        'alias',
        nargs='*',
        help="Optional aliases for the workspace"
    )
    ws_parser.add_argument(
        '-c', '--create',
        nargs='?',
        type=workspaceType,
        help="Create a new workspace. Default behavior without this flag is to"
        " fail if the workspace doesn't already exist in firecloud. You can "
        "optionally provide the name of a workspace (in lapdog) as an argument"
        " to this flag, and the new workspace will be cloned from the provided"
        " one",
        metavar="SOURCE",
        default=False,
        const=None
    )
    ws_parser.add_argument(
        '-n', '--no-save',
        action='store_true',
        help="Do not save any aliases. This is only useful for creating new workspaces."
        " Using -n without -c results in a no-op"
    )

    stats_parser = subparsers.add_parser(
        'stats',
        help="Collect stats for a configuration in the workspace",
        description="Collect stats for a configuration in the workspace",
        parents=[parent]
    )
    stats_parser.set_defaults(func=cmd_stats)
    stats_parser.add_argument(
        'configuration',
        help="Configuration to check"
    )

    upload_parser = subparsers.add_parser(
        'upload',
        help="Uploads participant data to a workspace",
        description="Uploads participant data to a workspace",
        parents=[parent]
    )
    upload_parser.set_defaults(func=cmd_upload)
    upload_parser.add_argument(
        'source',
        type=argparse.FileType('r'),
        help="CSV, TSV,  or json file to upload. CSV or TSV must have a header and must include"
        " sample_id and participant_id fields. JSON file must be an array of dicts"
        " or a dict of arrays. In either JSON schema, the dicts must contain "
        "sample_id and participant_id fields."
    )
    upload_parser.add_argument(
        '-f', '--files',
        action='store_true',
        help="Anything that looks like a local filepath will be uploaded to the"
        " workspace's bucket prior to uploading the samples"
    )

    method_parser = subparsers.add_parser(
        'method',
        help="Uploads a method or config to firecloud",
        description="Uploads a method or config to firecloud",
        parents=[parent]
    )
    method_parser.set_defaults(func=cmd_method)
    method_parser.add_argument(
        '-w', '--wdl',
        type=argparse.FileType('r'),
        help="WDL to upload. The method will be uploaded to the same namespace"
        " as the given workspace",
        default=None
    )
    method_parser.add_argument(
        '-n', '--method-name',
        help="The name of the uploaded method. This argument is ignored if the"
        " --wdl argument is not provided",
        default=None
    )
    method_parser.add_argument(
        '-c', '--config',
        type=argparse.FileType('r'),
        help="Configuration to upload",
        default=None
    )

    attributes_parser = subparsers.add_parser(
        'attributes',
        help="Sets attributes on the workspace",
        description="Sets attributes on the workspace",
        parents=[parent]
    )
    attributes_parser.set_defaults(func=cmd_attrs)
    attributes_parser.add_argument(
        'source',
        type=argparse.FileType('r'),
        help="JSON file to upload. The root object must be a dictionary"
    )
    attributes_parser.add_argument(
        '-f', '--files',
        action='store_true',
        help="Anything that looks like a local filepath will be uploaded to the"
        " workspace's bucket prior to uploading the samples"
    )

    exec_parser = subparsers.add_parser(
        'exec',
        help='Executes a configuration outside of firecloud. '
        'Execution will occur directly on GCP and outputs will be returned to firecloud',
        description='Executes a configuration outside of firecloud. '
        'Execution will occur directly on GCP and outputs will be returned to firecloud',
        parents=[parent]
    )
    exec_parser.set_defaults(func=cmd_exec)
    exec_parser.add_argument(
        'config',
        help="Configuration to run"
    )
    exec_parser.add_argument(
        'entity',
        help="The entity to run on. Entity is assumed to be of the same "
        "type as the configuration's root entity type. If you would like to "
        "run on a different entity type, use the --expression argument"
    )
    exec_parser.add_argument(
        '-x', '--expression',
        nargs=2,
        help="If the entity provided is not the same as the root entity type of"
        " the configuration, use this option to set a different entity type and"
        " entity expression. This option takes two arguments provide "
        "the new entity type followed by the expression for this entity",
        metavar=("ENTITY_TYPE", "EXPRESSION"),
        default=None
    )

    run_parser = subparsers.add_parser(
        'run',
        aliases=['submit'],
        help='Submits a job to run in firecloud',
        description='Submits a job to run in firecloud',
        parents=[exec_parser],
        conflict_handler='resolve'
    )
    run_parser.set_defaults(func=cmd_run)
    run_parser.add_argument(
        '-n', '--no-cache',
        action='store_true',
        help="Disables the use of the call cache in firecloud"
    )
    run_parser.add_argument(
        '-a', '--after',
        help="Do not run the submission immediately, and schedule it after the "
        "provided submission ID. You may alternatively provide a temporary id "
        "provided by lapdog to schedule this submission after one that has not "
        "yet started.",
        metavar="SUBMISSION_ID",
        default=None
    )

    sub_parser = subparsers.add_parser(
        'submissions',
        help="Get status of submissions in the workspace",
        description="Get status of submissions in the workspace",
        parents=[parent]
    )
    sub_parser.set_defaults(func=cmd_submissions)
    sub_parser.add_argument(
        '-i', '--id',
        # action='append',
        help="Display only the submission with this id",
        metavar='SUBMISSION_ID',
        default=None
    )
    sub_parser.add_argument(
        '-c', '--config',
        # action='append',
        help='Display only submissions with this configuration',
        default=None
    )
    sub_parser.add_argument(
        '-e', '--entity',
        # action='append',
        help="Display only submissions on this entity",
        default=None
    )
    sub_parser.add_argument(
        '-d', '--done',
        action='store_true',
        help="Display only submissions which have finished"
    )

    config_parser = subparsers.add_parser(
        'configurations',
        help="List all configurations in the workspace",
        description="List all configurations in the workspace",
        parents=[parent]
    )
    config_parser.set_defaults(func=cmd_configs)

    list_parser = subparsers.add_parser(
        'list',
        help="List all workspaces known to lapdog",
        description="List all workspaces known to lapdog"
    )
    list_parser.set_defaults(func=cmd_list)

    info_parser = subparsers.add_parser(
        'info',
        help="Display summary statistics for the workspace",
        description="Display summary statistics for the workspace",
        parents=[parent]
    )
    info_parser.set_defaults(func=cmd_info)

    test_parser = subparsers.add_parser(
        'test',
        help="Test run a configuration using local cromwell",
        description="Test run a configuration using local cromwell",
        parents=[parent]
    )
    test_parser.set_defaults(func=cmd_test)
    test_parser.add_argument(
        'config',
        help="Configuration to run"
    )
    test_parser.add_argument(
        'entity',
        help="The entity to run on. Entity is assumed to be of the same "
        "type as the configuration's root entity type. Test cannot run using an"
        " expression. It can only launch one workflow, which will run locally"
    )
    test_parser.add_argument(
        '-c', '--cromwell',
        type=argparse.FileType('rb'),
        help="Path to cromwell. Default: ./cromwell.jar",
        default='cromwell.jar'
    )

    finish_parser = subparsers.add_parser(
        'finish',
        help="Finishes an execution and uploads results to firecloud",
        description="Finishes an execution and uploads results to firecloud",
    )
    finish_parser.set_defaults(func=cmd_finish)
    finish_parser.add_argument(
        'submission',
        help="Lapdog submission id provided by 'lapdog exec'"
    )
    finish_parser.add_argument(
        '-s','--status',
        action='store_true',
        help="Show the status of the submission and exit without doing anything"
    )
    finish_parser.add_argument(
        '-a', '--abort',
        action='store_true',
        help="Abort the submission, if it hasn't finished already"
    )

    args = parser.parse_args()
    try:
        func = args.func
    except AttributeError:
        parser.print_usage()
        sys.exit("You must provide a valid subcommand")
    func(args)

def cmd_add_workspace(args):
    if args.no_save and not args.create:
        print("Warning: Using --no-save without --create results in a no-op")
    data = load_data()

    if 'workspaces' not in data:
        data['workspaces'] = {}

    if args.workspace in data['workspaces']:
        sys.exit('This workspace already exists')

    for name in [args.workspace] + args.alias:
        if '/' in name:
            sys.exit("Workspace name '%s' cannot contain '/'" % name)

    if not args.no_save:
        valid = [alias for alias in args.alias if alias not in data['workspaces']]
        if len(args.alias) and not len(valid):
            sys.exit('None of the provided aliases were available')
        elif len(valid) < len(args.alias):
            print("Warning: Not all aliases were available")

    ws = dalmatian.WorkspaceManager(args.namespace, args.workspace)
    try:
        ws.get_bucket_id()
        exists = True
    except AssertionError:
        exists = False
    if exists and args.create is not False:
        sys.exit("This workspace already exists")
    elif args.create is False and not exists:
        sys.exit("This workspace does not exist. Use the --create flag to create a new one")

    if args.create is not False:
        #args.create will either be a workspace type or None
        ws.create_workspace(wm=args.create)
    if not args.no_save:
        data = load_data()
        if 'workspaces' not in data:
            data['workspaces'] = {}
        for name in valid + [args.workspace]:
            data['workspaces'][name] = {
                'namespace': args.namespace,
                'workspace': args.workspace
            }
        with open(data_path, 'w') as writer:
            json.dump(data, writer, indent='\t')

def cmd_stats(args):
    try:
        print("Retrieving submission history...")
        sample_status = args.workspace.get_entity_status(None, args.configuration)
        print("Configuration name: ", np.unique(sample_status['configuration']))
        print("Gathering latest submission data...")
        status,_ = args.workspace.get_stats(sample_status)
        for sample, row in status.iterrows():
            if row.status != 'Succeeded':
                print(sample,row.status)
        print("Runtime:", max(status['time_h']), 'hours')
        print("CPU time:", sum(status['cpu_hours']), 'hours')
        print("Estimated cost: $", sum(status.query('est_cost == est_cost')['est_cost']), sep='')
    except AssertionError:
        # raise
        sys.exit("lapdog has encountered an error with firecloud. Please try again later")
    except:
        raise
        sys.exit("lapdog encountered an unexpected error")

def cmd_upload(args):
    """
    CSV or json file to upload. CSV must have a header and must include
    sample_id and participant_id fields. JSON file must be an array of dicts
    or a dict of arrays. In either JSON schema, the dicts must contain
    sample_id and participant_id fields.
    """

    if args.source.name.endswith('.csv') or args.source.name.endswith('.tsv'):
        reader = csv.DictReader(
            args.source,
            delimiter='\t' if args.source.name.endswith('.tsv') else ','
        )
        if 'sample_id' not in reader.fieldnames or 'participant_id' not in reader.fieldnames:
            sys.exit("Input source file must contain 'sample_id' and 'participant_id' fields")
        if args.files:
            samples = []
            pending_uploads = []
            bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
            (root, ext) = os.path.splitext(args.source.name)
            with open(root+'.lapdog'+ext, 'w') as w:
                writer = csv.DictWriter(w, reader.fieldnames, delimiter='\t' if args.source.name.endswith('.tsv') else ',', lineterminator='\n')
                writer.writeheader()
                for sample in reader:
                    for k,v in sample.items():
                        if os.path.isfile(v):
                            bucket_path = 'samples/%s/%s' % (
                                sample['sample_id'],
                                os.path.basename(v)
                            )
                            gs_path = 'gs://%s/%s' % (
                                args.workspace.get_bucket_id(),
                                bucket_path
                            )
                            print("Uploading", v, "to", gs_path)
                            pending_uploads.append(upload(bucket, bucket_path, v))
                            sample[k] = gs_path
                    writer.writerow(sample)
                    samples.append({k:v for k,v in sample.items()})
            _ = [callback() for callback in status_bar.iter(pending_uploads)]
        else:
            samples = list(reader)
        args.workspace.upload_data(samples)
    elif args.source.name.endswith('.json'):
        source = json.load(args.source)
        if type(source) == list and type(souce[0]) == dict and 'sample_id' in source[0] and 'participant_id' in source[0]:
            #standard
            if args.files:
                pending_uploads = []
                bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
                for i in len(source):
                    sample = source[i]
                    for k,v in sample.items():
                        if os.path.isfile(v):
                            bucket_path = 'samples/%s/%s' % (
                                sample['sample_id'],
                                os.path.basename(v)
                            )
                            gs_path = 'gs://%s/%s' % (
                                args.workspace.get_bucket_id(),
                                bucket_path
                            )
                            print("Uploading", v, "to", gs_path)
                            pending_uploads.append(upload(bucket, bucket_path, v))
                            source[i][k] = gs_path
                (root, ext) = os.path.splitext(args.source.name)
                with open(root+'.lapdog'+ext, 'w') as w:
                    json.dump(source, w, indent='\t')
                _ = [callback() for callback in status_bar.iter(pending_uploads)]
            args.workspace.upload_data(source)
        elif type(source) == dict and type(source[[k for k in source][0]]) == list and 'sample_id' in source and 'participant_id' in source:
            if args.files:
                pending_uploads = []
                bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
                for key in source:
                    for i in range(len(source[key])):
                        entry = source[key][i]
                        if os.path.isfile(entry):
                            bucket_path = 'samples/%s/%s' % (
                                source['sample_id'][i],
                                os.path.basename(entry)
                            )
                            gs_path = 'gs://%s/%s' % (
                                args.workspace.get_bucket_id(),
                                bucket_path
                            )
                            print("Uploading", entry, "to", gs_path)
                            pending_uploads.append(upload(bucket, bucket_path, entry))
                            # blob = bucket.blob(bucket_path)
                            # blob.upload_from_filename(entry)
                            source[key][i] = gs_path
                (root, ext) = os.path.splitext(args.source.name)
                with open(root+'.lapdog'+ext, 'w') as w:
                    json.dump(source, w, indent='\t')
                _ = [callback() for callback in status_bar.iter(pending_uploads)]
            args.workspace.upload_data(source, transpose=False)
    else:
        sys.exit("Please use a .tsv, .csv, or .json file")

def cmd_method(args):
    if args.wdl is None and args.config is None:
        sys.exit("Must provide either a method or configuration")

    if args.wdl:
        name = args.method_name if args.method_name is not None else os.path.splitext(os.path.basename(args.wdl.name))[0]
        dalmatian.update_method(
            args.workspace.namespace,
            name,
            "Runs " + name,
            args.wdl.name
        )
    if args.config:
        args.workspace.update_configuration(json.load(args.config))


def cmd_attrs(args):

    source = json.load(args.source)
    if type(source) != dict:
        sys.exit("The root object must be a dictionary")
    if args.files:
        pending_uploads = []
        bucket = storage.Client().get_bucket(args.workspace.get_bucket_id())
        source, pending_uploads = walk_and_upload(bucket, source)
        (root, ext) = os.path.splitext(args.source.name)
        with open(root+'.lapdog'+ext, 'w') as w:
            json.dump(source, w, indent='\t')
        _ = [callback() for callback in status_bar.iter(pending_uploads)]
    args.workspace.update_attributes(source)

def walk_and_upload(bucket, obj):
    output = []
    if type(obj) == dict:
        for key,val in obj.items():
            if type(val) in {dict, list}:
                obj[key], tmp = walk_and_upload(bucket, val)
                output += tmp
            elif type(val) == str and os.path.isfile(val):
                bucket_path = 'workspace/%s' % (
                    os.path.basename(val)
                )
                gs_path = 'gs://%s/%s' % (
                    bucket.id,
                    bucket_path
                )
                output.append(upload(bucket, bucket_path, val))
                obj[key] = gs_path
    elif type(obj) == list:
        for key in range(len(obj)):
            val = obj[key]
            if type(val) in {dict, list}:
                obj[key], tmp = walk_and_upload(bucket, prefix, val)
                output += tmp
            elif type(val) == str and os.path.isfile(val):
                bucket_path = 'workspace/%s' % (
                    os.path.basename(val)
                )
                gs_path = 'gs://%s/%s' % (
                    bucket.id,
                    bucket_path
                )
                output.append(upload(bucket, bucket_path, val))
                obj[key] = gs_path
    return (obj, output)


def cmd_run(args):
    configs = {
        config['name']:config for config in
        dalmatian.firecloud.api.list_workspace_configs(
            args.workspace.namespace,
            args.workspace.workspace
        ).json()
    }
    if args.config not in configs:
        print(
            "Configurations found in this workspace:",
            [config for config in configs]
        )
        sys.exit("Configuration '%s' does not exist" % args.config)
    args.config = configs[args.config]
    etype = args.expression[0] if args.expression is not None else args.config['rootEntityType']
    # entities_df = args.workspace.get_entities(etype)
    # try:
    #     entity = entities_df.loc[args.entity]
    # except KeyError:
    #     sys.exit("%s '%s' not found in workspace" % (
    #         etype.title(),
    #         args.entity
    #     ))
    response = dalmatian.firecloud.api.get_entity(
        args.workspace.namespace,
        args.workspace.workspace,
        etype,
        args.entity
    )
    if response.status_code >= 400 and response.status_code <500:
        sys.exit("%s '%s' not found in workspace" % (
            etype.title(),
            args.entity
        ))
    elif response.status_code >= 500:
        sys.exit("Encountered an unexpected error with the firecloud api")
    data = load_data()
    tmp_id = 'tmp::'+md5(str(time.time()).encode()).hexdigest()
    if 'submissions' in data:
        while tmp_id in data['submissions']:
            tmp_id = 'tmp::'+md5(tmp_id.encode()).hexdigest()
    if args.after is not None:
        print("Temporary ID:", tmp_id)
        print(
            "You may use that in place of a submission id to schedule submissions"
            " after this one"
        )
        if args.after.startswith('tmp::'):
            print("Waiting for lapdog to start the submission for ", args.after)
            count = 0
            while not ('submissions' in data and args.after in data['submissions']):
                if count < 5:
                    time.sleep(60)
                elif count < 10:
                    time.sleep(120)
                elif count < 20:
                    time.sleep(300)
                else:
                    time.sleep(1800)
                count += 1
                data = load_data()
            print(
                "The submission for",
                args.after,
                "has been started:",
                data['submissions'][args.after]
            )
            args.after = data['submissions'][args.after]
        response = dalmatian.firecloud.api.get_submission(
            args.workspace.namespace,
            args.workspace.workspace,
            args.after
        )
        if response.status_code not in {200,201}:
            sys.exit("Failed to find submission ID: "+response.text)
        running = response.json()['status'] != 'Done'
        succeeded = True
        for workflow in response.json()['workflows']:
            if workflow['status'] != 'Succeeded':
                succeeded = False
        if not (running or succeeded):
            sys.exit("The provided workflow ID has failed")
        elif running:
            print("Waiting for the submission to finish")
            count = 0
            while running:
                if count < 5:
                    time.sleep(60)
                elif count < 10:
                    time.sleep(120)
                elif count < 20:
                    time.sleep(300)
                else:
                    time.sleep(1800)
                count += 1
                response = dalmatian.firecloud.api.get_submission(
                    args.workspace.namespace,
                    args.workspace.workspace,
                    args.after
                )
                if response.status_code not in {200,201}:
                    sys.exit("Failed to find submission ID: "+response.text)
                running = response.json()['status'] != 'Done'
                succeeded = True
                for workflow in response.json()['workflows']:
                    if workflow['status'] != 'Succeeded':
                        succeeded = False
        if not succeeded:
            sys.exit("The provided workflow ID has failed")
    print("Creating submission")
    result = args.workspace.create_submission(
        args.config['namespace'],
        args.config['name'],
        args.entity,
        etype,
        expression=args.expression[1] if args.expression is not None else None,
        use_callcache=not args.no_cache
    )
    if result is None:
        sys.exit("Failed to create submission")
    data = load_data()
    if 'submissions' not in data:
        data['submissions'] = {}
    data['submissions'][tmp_id]=result
    with open(data_path, 'w') as writer:
        json.dump(data, writer, indent='\t')

def cmd_submissions(args):
    submissions = args.workspace.get_submission_status(filter_active=args.done)
    if args.id:
        submissions=submissions[
            submissions['submission_id'] == args.id
        ]
    if args.config:
        submissions=submissions[
            submissions['configuration'] == args.config
        ]
    if args.entity:
        submissions=submissions[
            submissions.index == args.entity
        ]
    print(submissions)

def cmd_configs(args):

    print("Configurations in this workspace:")
    print("Configuration\tSynopsis")
    for config in dalmatian.firecloud.api.list_workspace_configs(args.workspace.namespace, args.workspace.workspace).json():
        config_data = dalmatian.firecloud.api.get_repository_method(
            config['methodRepoMethod']['methodNamespace'],
            config['methodRepoMethod']['methodName'],
            config['methodRepoMethod']['methodVersion']
        ).json()
        print(
            config['name'],
            config_data['synopsis'] if 'synopsis' in config_data else '',
            sep='\t'
        )

def cmd_list(args):
    data = load_data()

    if 'workspaces' not in data:
        data['workspaces'] = {}

    workspaces = {}
    for alias, workspace in data['workspaces'].items():
        namespace = workspace['namespace']
        workspace = workspace['workspace']
        if namespace not in workspaces:
            workspaces[namespace] = {workspace: [alias]}
        elif workspace not in workspaces[namespace]:
            workspaces[namespace][workspace] = [alias]
        else:
            workspaces[namespace][workspace].append(alias)

    print("Firecloud Workspace\tLapdog Workspace Names")
    print()
    for namespace in workspaces:
        for workspace in workspaces[namespace]:
            first = True
            name = '%s/%s'%(namespace, workspace)
            for alias in workspaces[namespace][workspace]:
                print('%s\t%s'%(
                    name if first else ' '*len(name),
                    alias
                ))
                first = False

def cmd_info(args):

    submissions = args.workspace.get_submission_status()
    bins = {}
    for row in submissions.iterrows():
        entity = row[0]
        row = row[1]
        if row.status not in bins:
            bins[row.status]={row.configuration:[(row.submission_id, entity)]}
        elif row.configuration not in bins[row.status]:
            bins[row.status][row.configuration]=[(row.submission_id, entity)]
        else:
            bins[row.status][row.configuration].append((row.submission_id, entity))

    # print("Lapdog Workspace:",args.workspace)
    print("Firecloud Workspace:", '%s/%s' % (args.workspace.namespace, args.workspace.workspace))
    print("Submissions:")
    for status in bins:
        print(status,sum(len(item) for item in bins[status].values()),"submission(s):")
        for configuration in bins[status]:
            print('\t'+configuration, len(bins[status][configuration]), 'submission(s):')
            for submission, entity in bins[status][configuration]:
                print('\t\t'+submission, '(%s)'%entity)

def cmd_test(args):
    # 1) resolve configuration
    configs = {
        config['name']:config for config in
        dalmatian.firecloud.api.list_workspace_configs(
            args.workspace.namespace,
            args.workspace.workspace
        ).json()
    }
    if args.config not in configs:
        print(
            "Configurations found in this workspace:",
            [config for config in configs]
        )
        sys.exit("Configuration '%s' does not exist" % args.config)
    args.config = configs[args.config]
    # 2) Verify that the given target matches the root entity type (no expressions, 1 workflow only)
    etype = args.config['rootEntityType']
    response = dalmatian.firecloud.api.get_entity(
        args.workspace.namespace,
        args.workspace.workspace,
        etype,
        args.entity
    )
    if response.status_code >= 400 and response.status_code <500:
        sys.exit("%s '%s' not found in workspace" % (
            etype.title(),
            args.entity
        ))
    elif response.status_code >= 500:
        sys.exit("Encountered an unexpected error with the firecloud api")
    # 3) Prepare config template
    print("Resolving inputs")
    # tempdir = tempfile.TemporaryDirectory()
    tempdir = lambda x:None
    tempdir.name = '.'
    downloads = []
    template = dalmatian.firecloud.api.get_workspace_config(
        args.workspace.namespace,
        args.workspace.workspace,
        args.config['namespace'],
        args.config['name']
    ).json()['inputs']

    invalid_inputs = dalmatian.firecloud.api.validate_config(
        args.workspace.namespace,
        args.workspace.workspace,
        args.config['namespace'],
        args.config['name']
    ).json()['invalidInputs']

    if len(invalid_inputs):
        print("The following input fields are invalid:", list(invalid_inputs))

    for k in list(template):
        if k not in invalid_inputs:
            v = template[k]
            resolution = dalmatian.firecloud.api.__post(
                'workspaces/%s/%s/entities/%s/%s/evaluate' % (
                    args.workspace.namespace,
                    args.workspace.workspace,
                    etype,
                    args.entity
                ),
                data=v
            ).json()
            do_download = False
            if len(resolution) and isinstance(resolution[0], str) and resolution[0].startswith('gs://'):
                print('----')
                print("Input name:", k)
                print("Expression: ", v)
                print("Resolution:", resolution[0], '... (%d items)' % (len(resolution) - 1) if len(resolution)>1 else '')
                choice = input("Download this input? (Y/N): ").lower()
                do_download = len(choice) and choice[0] == 'y'
            for i in range(len(resolution)):
                if do_download and isinstance(resolution[i], str) and resolution[i].startswith('gs://'):
                    local_path = os.path.join(tempdir.name, os.path.basename(resolution[i]))
                    blob = getblob(resolution[i])
                    blobsize = blob.size
                    print(
                        "Downloading",
                        resolution[i],
                        (
                            '(%s)' % byteSize(blobsize)
                            if blobsize is not None
                            else ''
                        )
                    )
                    downloads.append(download(blob, local_path))
                    resolution[i] = local_path
            if len(resolution) == 1:
                template[k] = resolution[0]
            else:
                template[k] = resolution
        else:
            del template[k]
    if len(downloads):
        for callback in status_bar.iter(downloads, prepend="Downloading files "):
            callback()
    # 4) Download wdl
    with open(os.path.join(tempdir.name, 'method.wdl'), 'w') as writer:
        writer.write(dalmatian.get_wdl(
            args.config['methodRepoMethod']['methodNamespace'],
            args.config['methodRepoMethod']['methodName']
        ))

    if len(template):
        with open(os.path.join(tempdir.name, 'config.json'), 'w') as writer:
            json.dump(template, writer)

    # 5) Cromwell
    print("java -jar %s run %s %s" % (
        args.cromwell.name,
        os.path.join(tempdir.name, 'method.wdl'),
        os.path.join(tempdir.name, 'config.json') if len(template) else ''
    ))
    subprocess.run(
        "java -jar %s run %s %s" % (
            args.cromwell.name,
            os.path.join(tempdir.name, 'method.wdl'),
            os.path.join(tempdir.name, 'config.json') if len(template) else ''
        ),
        shell=True,
        executable='/bin/bash'
    )


def cmd_exec(args):
    # 1) resolve configuration
    configs = {
        config['name']:config for config in
        dalmatian.firecloud.api.list_workspace_configs(
            args.workspace.namespace,
            args.workspace.workspace
        ).json()
    }
    if args.config not in configs:
        print(
            "Configurations found in this workspace:",
            [config for config in configs]
        )
        sys.exit("Configuration '%s' does not exist" % args.config)
    args.config = configs[args.config]
    # 2) Verify that the given target matches the root entity type (no expressions, 1 workflow only)
    etype = args.expression[0] if args.expression is not None else args.config['rootEntityType']
    response = dalmatian.firecloud.api.get_entity(
        args.workspace.namespace,
        args.workspace.workspace,
        etype,
        args.entity
    )
    if response.status_code >= 400 and response.status_code <500:
        sys.exit("%s '%s' not found in workspace" % (
            etype.title(),
            args.entity
        ))
    elif response.status_code >= 500:
        sys.exit("Encountered an unexpected error with the firecloud api")

    print("Resolving wrokflow entities")
    workflow_entities = dalmatian.firecloud.api.__post(
        'workspaces/%s/%s/entities/%s/%s/evaluate' % (
            args.workspace.namespace,
            args.workspace.workspace,
            etype,
            args.entity
        ),
        data=(
            args.expression[1]
            if args.expression is not None
            else 'this'
        )+'.%s_id' % args.config['rootEntityType']
    ).json()
    print("This will launch", len(workflow_entities), "workflow(s)")

    # 3) Prepare config template
    # print("Resolving inputs")

    template = dalmatian.firecloud.api.get_workspace_config(
        args.workspace.namespace,
        args.workspace.workspace,
        args.config['namespace'],
        args.config['name']
    ).json()['inputs']

    invalid_inputs = dalmatian.firecloud.api.validate_config(
        args.workspace.namespace,
        args.workspace.workspace,
        args.config['namespace'],
        args.config['name']
    ).json()['invalidInputs']

    if len(invalid_inputs):
        print("The following input fields are invalid:", list(invalid_inputs))

    submission_id = md5((str(time.time()) + args.config['name'] + args.entity).encode()).hexdigest()
    print("Submission ID for this job:", submission_id)
    # print("Debug info:")
    # print("argument template:", json.dumps(template, indent='\t'))
    # print("Config entity type:", etype)
    # print("Entities:", workflow_entities[:5],'...')
    print("Ready to launch workflow(s). Press Enter to continue")
    input()

    submission_data = {
        'workflows':[],
        'workspace':args.workspace.workspace,
        'namespace':args.workspace.namespace,
        'config':args.config['name'],
        'workflow_entity_type': args.config['rootEntityType'],
        'entity':args.entity,
        'etype':etype,
        'expression':args.expression[1] if args.expression is not None else None
    }

    @parallelize(5)
    def launch_workflow(args, submission_id, template, invalid, etype, entity):
        workflow_id = submission_id[:8]+"-"+md5((str(time.time()) + args.config['name'] + entity).encode()).hexdigest()
        workflow_template = {}
        for key, val in template.items():
            if key not in invalid:
                pass
            resolution = dalmatian.firecloud.api.__post(
                'workspaces/%s/%s/entities/%s/%s/evaluate' % (
                    args.workspace.namespace,
                    args.workspace.workspace,
                    etype,
                    entity
                ),
                data=val
            ).json()
            if len(resolution) == 1:
                workflow_template[key] = resolution[0]
            else:
                workflow_template[key] = resolution
        tempdir = tempfile.TemporaryDirectory()
        with open(os.path.join(tempdir.name, 'method.wdl'),'w') as w:
            w.write(dalmatian.get_wdl(
                args.config['methodRepoMethod']['methodNamespace'],
                args.config['methodRepoMethod']['methodName']
            ))
        with open(os.path.join(tempdir.name, 'config.json'), 'w') as w:
            json.dump(workflow_template, w, indent='\t')
        with open(os.path.join(tempdir.name, 'options.json'), 'w') as w:
            json.dump(
                {
                    'default_runtime_attributes': {
                        'zones': 'us-east4-a',
                        'labels': 'lapdog-submission-id={},lapdog-workflow-id={}'.format(submission_id, workflow_id)
                    }
                },
                w
            )
        # print("Debug info:")
        # with open(os.path.join(tempdir.name, 'method.wdl')) as r:
        #     print("WDL:", r.read()[:512])
        # with open(os.path.join(tempdir.name, 'config.json')) as r:
        #     print("Config:", r.read())
        cmd = (
            'gcloud alpha genomics pipelines run '
            '--pipeline-file /Users/aarong/Documents/wdlparser/wdl/runners/cromwell_on_google/wdl_runner/wdl_pipeline.yaml '
            '--zones {zone} '
            '--inputs-from-file WDL={wdl_text} '
            '--inputs-from-file WORKFLOW_INPUTS={workflow_template} '
            '--inputs-from-file WORKFLOW_OPTIONS={options_template} '
            '--inputs WORKSPACE=gs://{bucket_id}/lapdog-executions/{submission_id}/{workflow_id}/workspace '
            '--inputs OUTPUTS=gs://{bucket_id}/lapdog-executions/{submission_id}/{workflow_id}/results '
            '--logging gs://{bucket_id}/lapdog-executions/{submission_id}/{workflow_id}/logs '
            # '--inputs WORKSPACE=gs://cga-aarong-resources/lapdog-executions/{submission_id}/{workflow_id}/workspace '
            # '--inputs OUTPUTS=gs://cga-aarong-resources/lapdog-executions/{submission_id}/{workflow_id}/results '
            # '--logging gs://cga-aarong-resources/lapdog-executions/{submission_id}/{workflow_id}/logs '
            '--labels lapdog-submission-id={submission_id},lapdog-workflow-id={workflow_id} '
            '--service-account-scopes=https://www.googleapis.com/auth/devstorage.full_control'
        ).format(
            zone='us-east4-a',
            wdl_text=os.path.join(tempdir.name, 'method.wdl'),
            workflow_template=os.path.join(tempdir.name, 'config.json'),
            options_template=os.path.join(tempdir.name, 'options.json'),
            bucket_id=args.workspace.get_bucket_id(),
            submission_id=submission_id,
            workflow_id=workflow_id
        )
        # print(cmd)
        results = subprocess.run(
            cmd, shell=True, executable='/bin/bash',
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        ).stdout.decode()
        time.sleep(10)
        # print("RETURN:", results)
        return {
            'workflow_id': workflow_id,
            'workflow_output':'gs://{}/lapdog-executions/{}/{}/results/wdl_run_metadata.json'.format(
                args.workspace.get_bucket_id(),
                submission_id,
                workflow_id
            ),
            'workflow_entity': entity,
            # 'operation_id': '<PLACEHOLDER>'
            'operation_id': re.search(
                r'(operations/\S+)\].',
                results
            ).group(1)
        }

    for workflow_inputs in status_bar.iter(launch_workflow(repeat(args), repeat(submission_id), repeat(template), repeat(invalid_inputs), repeat(args.config['rootEntityType']), workflow_entities), len(workflow_entities), prepend="Launching workflows... "):
        submission_data['workflows'].append(workflow_inputs)

    # print(json.dumps(submission_data, indent='\n'))

    data = load_data()
    if 'executions' not in data:
        data['executions'] = {}
    data['executions'][submission_id] = submission_data
    with open(data_path, 'w') as writer:
        json.dump(data, writer, indent='\t')

    print("Done! Use 'lapdog finish %s' to upload the results after the job finishes" %submission_id)

#if wdl runner works, just spin up a runner instance and submit the job
#otherwise:
#for submission expression, assume 1+ workflows will be created
# evaluate {expression}.{root_entity_type}_id to create workflows

@parallelize()
def get_operation_status(opid):
    return yaml.load(
        StringIO(
            subprocess.run(
                'gcloud alpha genomics operations describe %s' % (
                    opid
                ),
                shell=True,
                executable='/bin/bash',
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            ).stdout.decode()
        )
    )

@parallelize()
def abort_operation(opid):
    return subprocess.run(
        'yes | gcloud alpha genomics operations cancel %s' % (
            opid
        ),
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

def cmd_finish(args):
    data = load_data()
    if 'executions' not in data:
        data['executions'] = {}
    if args.submission not in data['executions']:
        sys.exit("No such submission: "+args.submission)
    submission = data['executions'][args.submission]

    workflow_data = [*status_bar.iter(get_operation_status([
        wf['operation_id'] for wf in submission['workflows']
    ]), len(submission['workflows']), prepend="Checking status of %d operations " % len(submission['workflows']))]
    done = 0
    for i in range(len(workflow_data)):
        submission['workflows'][i]['data'] = workflow_data[i]
        if 'done' in workflow_data[i] and workflow_data[i]['done']:
            done += 1

    print("Completed ", done,'/',len(submission['workflows']), " workflows", sep='')

    if args.status:
        return
    if args.abort:
        results = [*status_bar.iter(abort_operation([
            wf['operation_id'] for wf in submission['workflows']
        ]), len(submission['workflows']), prepend="Aborting workflows... ")]
        for result, wf in zip(results, submission['workflows']):
            if result.returncode and not ('done' in wf['data'] and wf['data']['done']):
                print("Failed to abort:", wf['operation_id'])
        return
    if done >= len(submission['workflows']):
        print("All workflows completed. Uploading results...")
        ws = dalmatian.WorkspaceManager(
            submission['namespace'],
            submission['workspace']
        )

        output_template = dalmatian.firecloud.api.get_workspace_config(
            submission['namespace'],
            submission['workspace'],
            submission['namespace'],
            submission['config']
        ).json()['outputs']

        print("Output template:", output_template)
        output_data = {}
        for wf in status_bar.iter(submission['workflows']):
            if 'error' in wf['data']:
                print("Workflow", wf['workflow_id'], "failed")
                print("Entity:", wf['workflow_entity'])
                print("Error:", wf['data']['error'])
            else:
                output_data = json.loads(getblob(wf['workflow_output']).download_as_string())
                entity_data = {}
                for k,v in output_data['outputs'].items():
                    k = output_template[k]
                    if k.startswith('this.'):
                        entity_data[k[5:]] = v
                ws.update_entity_attributes(
                    submission['workflow_entity_type'],
                    pd.DataFrame(
                        entity_data,
                        index=[wf['workflow_entity']]
                    ),
                    quiet=True
                )


    # submission_data = {
    #     'workflows':[],
    #     'workspace':args.workspace.workspace,
    #     'namespace':args.workspace.namespace,
    #     'config':args.config['name'],
    #     'entity':args.entity,
    #     'etype':etype,
    #     'expression':args.expression[1] if args.expression is not None else None
    # }

if __name__ == '__main__':
    main()
