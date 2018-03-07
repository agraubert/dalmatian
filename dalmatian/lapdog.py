import argparse
import dalmatian
import pandas as pd
import json
import os
import numpy as np
import sys
from google.cloud import storage
import csv
from agutil.parallel import parallelize2
from agutil import status_bar
from hashlib import md5
import time

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

    run_parser = subparsers.add_parser(
        'run',
        help='Runs a configuration in the workspace',
        description='Runs a configuration in the workspace',
        parents=[parent]
    )
    run_parser.set_defaults(func=cmd_run)
    run_parser.add_argument(
        'config',
        help="Configuration to run"
    )
    run_parser.add_argument(
        'entity',
        help="The entity to run on. Entity is assumed to be of the same "
        "type as the configuration's root entity type. If you would like to "
        "run on a different entity type, use the --expression argument"
    )
    run_parser.add_argument(
        '-x', '--expression',
        nargs=2,
        help="If the entity provided is not the same as the root entity type of"
        " the configuration, use this option to set a different entity type and"
        " entity expression. This option takes two arguments provide "
        "the new entity type followed by the expression for this entity",
        metavar=("ENTITY_TYPE", "EXPRESSION"),
        default=None
    )
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

    args = parser.parse_args()
    try:
        func = args.func
    except AttributeError:
        parser.print_usage()
        sys.exit("You must provide a valid subcommand")
    func(args)

def cmd_add_workspace(args):
    data = load_data()

    if 'workspaces' not in data:
        data['workspaces'] = {}

    if args.workspace in data['workspaces']:
        sys.exit('This workspace already exists')

    for name in [args.workspace] + args.alias:
        if '/' in name:
            sys.exit("Workspace name '%s' cannot contain '/'" % name)

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
    "CSV or json file to upload. CSV must have a header and must include"
    " sample_id and participant_id fields. JSON file must be an array of dicts"
    " or a dict of arrays. In either JSON schema, the dicts must contain "
    "sample_id and participant_id fields."

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
                writer = csv.DictWriter(w, reader.fieldnames, delimiter=reader.delimiter, lineterminator='\n')
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
        souce = json.load(args.source)
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
    entities_df = args.workspace.get_entities(etype)
    try:
        entity = entities_df.loc[args.entity]
    except KeyError:
        sys.exit("%s '%s' not found in workspace" % (
            etype.title(),
            args.entity
        ))
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
            submissions['submission_id']==args.id
        ]
    if args.config:
        submissions=submissions[
            submissions['configuration']==args.config
        ]
    if args.entity:
        submissions=submissions[
            submissions.index==args.entity
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
            workspaces[namespace]={workspace:[alias]}
        elif workspace not in workspaces[namespace]:
            workspaces[namespace][workspace]=[alias]
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

    print("Lapdog Workspace:",args.workspace)
    print("Firecloud Workspace:", '%s/%s' % (args.workspace.namespace, args.workspace.workspace))
    print("Submissions:")
    for status in bins:
        print(status,sum(len(item) for item in bins[status].values()),"submission(s):")
        for configuration in bins[status]:
            print('\t'+configuration, len(bins[status][configuration]), 'submission(s):')
            for submission, entity in bins[status][configuration]:
                print('\t\t'+submission, '(%s)'%entity)

if __name__ == '__main__':
    main()
