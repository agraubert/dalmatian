# Author: Francois Aguet

import os, sys, io, json
import subprocess
from datetime import datetime
from collections import Iterable
import pandas as pd
import numpy as np
import firecloud.api
import iso8601
import pytz
import matplotlib.pyplot as plt
from collections import defaultdict
import multiprocessing as mp
import argparse
from agutil.parallel import parallelize

from .__about__ import __version__

# Collection of high-level wrapper functions for FireCloud API

def convert_time(x):
    return datetime.timestamp(iso8601.parse_date(x))


def workflow_time(workflow):
    """
    Convert API output to timestamp difference
    """
    if 'end' in workflow:
        return convert_time(workflow['end']) - convert_time(workflow['start'])
    else:
        return np.NaN


def gs_list_bucket_files(bucket_id):
    """
    Get list of all files stored in bucket
    """
    s = subprocess.check_output('gsutil ls gs://{}/**'.format(bucket_id), shell=True, executable='/bin/bash')
    return s.decode().strip().split('\n')


def gs_delete(file_list, chunk_size=500):
    """
    Delete list of files (paths starting with gs://)
    """
    # number of calls is limited by command line size limit
    n = int(np.ceil(len(file_list)/chunk_size))
    for i in range(n):
        x = file_list[chunk_size*i:chunk_size*(i+1)]
        cmd = 'echo -e "{}" | gsutil -m rm -I'.format('\n'.join(x))
        subprocess.call(cmd, shell=True, executable='/bin/bash')


def gs_copy(file_list, dest_dir, chunk_size=500):
    """
    Copy list of files (paths starting with gs://)
    """
    n = int(np.ceil(len(file_list)/chunk_size))
    for i in range(n):
        x = file_list[chunk_size*i:chunk_size*(i+1)]
        cmd = 'echo -e "{}" | gsutil -m cp -I {}'.format('\n'.join(x), dest_dir)
        subprocess.check_call(cmd, shell=True, executable='/bin/bash')


def gs_move(file_list, dest_dir, chunk_size=500):
    """
    Move list of files (paths starting with gs://)
    """
    n = int(np.ceil(len(file_list)/chunk_size))
    for i in range(n):
        x = file_list[chunk_size*i:chunk_size*(i+1)]
        cmd = 'echo -e "{}" | gsutil -m mv -I {}'.format('\n'.join(x), dest_dir)
        subprocess.check_call(cmd, shell=True, executable='/bin/bash')


def gs_exists(file_list_s):
    """
    Check whether files exist
    """
    status_s = pd.Series(False, index=file_list_s.index, name='file_exists')
    for k,(i,p) in enumerate(zip(file_list_s.index, file_list_s)):
        print('\rChecking {}/{} files'.format(k+1, len(file_list_s)), end='')
        try:
            s = subprocess.check_output('gsutil -q stat {}'.format(p), shell=True)
            status_s[i] = True
        except subprocess.CalledProcessError as e:
            s = e.stdout.decode()
            print('{}: {}'.format(i, s))
    return status_s


class WorkspaceCollection(object):
    def __init__(self):
        self.workspace_list = []

    def add(self, workspace_manager):
        assert isinstance(workspace_manager, WorkspaceManager)
        self.workspace_list.append(workspace_manager)

    def remove(self, workspace_manager):
        self.workspace_list.remove(workspace_manager)

    def print_workspaces(self):
        print('Workspaces in collection:')
        for i in self.workspace_list:
            print('  {}/{}'.format(i.namespace, i.workspace))

    def get_submission_status(self, show_namespaces=False):
        """
        Get status of all submissions across workspaces
        """
        dfs = []
        for i in self.workspace_list:
            df = i.get_submission_status(show_namespaces=show_namespaces)
            if show_namespaces:
                df['workspace'] = '{}/{}'.format(i.namespace, i.workspace)
            else:
                df['workspace'] = i.workspace
            dfs.append(df)
        return pd.concat(dfs, axis=0)


class WorkspaceManager(object):
    def __init__(self, namespace, workspace, timezone='America/New_York'):
        self.namespace = namespace
        self.workspace = workspace
        self.timezone  = timezone


    def create_workspace(self, wm=None):
        """
        Wrapper for firecloud.api.create_workspace
        """
        if wm is None:
            r = firecloud.api.create_workspace(self.namespace, self.workspace)
            if r.status_code==201:
                print('Workspace {}/{} successfully created.'.format(self.namespace, self.workspace))
            elif r.status_code==409:
                print(r.json()['message'])
            else:
                print(r.json())
        else:  # clone workspace
            r = firecloud.api.clone_workspace(wm.namespace, wm.workspace, self.namespace, self.workspace)
            if r.status_code==201:
                print('Workspace {}/{} successfully cloned from {}/{}.'.format(self.namespace, self.workspace, wm.namespace, wm.workspace))
            else:
                print(r.json())


    def delete_workspace(self):
        r = firecloud.api.delete_workspace(self.namespace, self.workspace)
        if r.status_code==202:
            print('Workspace {}/{} successfully deleted.'.format(self.namespace, self.workspace))
            print('  * '+r.json()['message'])
        else:
            print(r.text)


    def get_bucket_id(self):
        r = firecloud.api.get_workspace(self.namespace, self.workspace)
        assert r.status_code==200
        r = r.json()
        bucket_id = r['workspace']['bucketName']
        return bucket_id


    def upload_samples(self, df, participant_df=None, add_participant_samples=False):
        """
        Upload samples stored in a pandas DataFrame, and populate the required
        participant, sample, and sample_set attributes

        df columns: sample_id (index), participant_id, {sample_set_id,} other attributes
        """
        assert df.index.name=='sample_id' and df.columns[0]=='participant_id'

        # 1) upload participant IDs (without additional attributes)
        if participant_df is None:
            participant_ids = np.unique(df['participant_id'])
            participant_df = pd.DataFrame(data=participant_ids, columns=['entity:participant_id'])
        else:
            assert participant_df.index.name=='entity:participant_id' or participant_df.columns[0]=='entity:participant_id'

        buf = io.StringIO()
        participant_df.to_csv(buf, sep='\t', index=participant_df.index.name=='entity:participant_id')
        s = firecloud.api.upload_entities_tsv(self.namespace, self.workspace, buf)
        buf.close()
        assert s.status_code==200
        print('Succesfully imported {} participants.'.format(participant_df.shape[0]))

        # 2) upload samples
        sample_df = df[df.columns[df.columns!='sample_set_id']].copy()
        sample_df.index.name = 'entity:sample_id'
        buf = io.StringIO()
        sample_df.to_csv(buf, sep='\t')
        s = firecloud.api.upload_entities_tsv(self.namespace, self.workspace, buf)
        buf.close()
        assert s.status_code==200
        print('Succesfully imported {} samples.'.format(sample_df.shape[0]))

        # 3 upload sample sets
        if 'sample_set_id' in df.columns:
            set_df = pd.DataFrame(data=sample_df.index.values, columns=['sample_id'])
            set_df.index = df['sample_set_id']
            set_df.index.name = 'membership:sample_set_id'
            buf = io.StringIO()
            set_df.to_csv(buf, sep='\t')
            s = firecloud.api.upload_entities_tsv(self.namespace, self.workspace, buf)
            buf.close()
            assert s.status_code==200
            print('Succesfully imported {} sample sets.'.format(set_df.shape[0]))

        if add_participant_samples:
            # 4) add participant.samples_
            print('  * The FireCloud data model currently does not provide participant.samples\n    Adding "participant.samples_" as an explicit attribute.')
            self.update_participant_samples()


    def upload_participants(self, participant_ids):
        """
        Upload samples stored in a pandas DataFrame, and populate the required
        participant, sample, and sample_set attributes

        df columns: sample_id, participant_id, {sample_set_id,} other attributes
        """
        participant_df = pd.DataFrame(data=np.unique(participant_ids), columns=['entity:participant_id'])
        buf = io.StringIO()
        participant_df.to_csv(buf, sep='\t', index=participant_df.index.name=='entity:participant_id')
        s = firecloud.api.upload_entities_tsv(self.namespace, self.workspace, buf)
        buf.close()
        assert s.status_code==200
        print('Succesfully imported participants.')


    def upload_data(self, samples, transpose=True):
        """
        Upload samples stored as an array of dicts into a workspace
        samples = [{sample_id:..., participant_id:..., key:val, etc}]
        If transpose is false, samples is taken as a dict of arrays:
        samples = {sample_id:[...], participant_id:[...], key:[...], etc}
        """
        if transpose:
            assert 'sample_id' in samples[0], "sample_id not defined in samples array"
            assert 'participant_id' in samples[0], "participant_id not defined in samples array"
            data = {'participant_id':[]}
            index = []
            for sample in samples:
                for key,value in sample.items():
                    if key == 'sample_id':
                        index.append(value)
                    elif key not in data:
                        data[key] = [value]
                    else:
                        data[key].append(value)
        else:
            assert 'sample_id' in samples, "sample_id not defined in samples dict"
            assert 'participant_id' in samples, "participant_id not defined in samples dict"
            data = {
                key:[val for val in entries] for key,entries in samples.items() if key != 'sample_id'
            }
            index = [entry for entry in samples['sample_id']]
        df = pd.DataFrame(
            index=index,
            data=data,
            columns=['participant_id']+[col for col in data if col != 'participant_id']
        )
        df.index.name = 'sample_id'
        self.upload_samples(df, add_participant_samples=True)


    def update_participant_samples(self):
        """
        Attach samples to participants
        """
        df = self.get_samples()[['participant']]
        samples_dict = {k:g.index.values for k,g in df.groupby('participant')}

        participant_ids = np.unique(df['participant'])
        for j,k in enumerate(participant_ids):
            print('\r    Updating samples for participant {}/{}'.format(j+1,len(participant_ids)), end='')
            attr_dict = {
                "samples_": {
                    "itemsType": "EntityReference",
                    "items": [{"entityType": "sample", "entityName": i} for i in samples_dict[k]]
                }
            }
            attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
            r = firecloud.api.update_entity(self.namespace, self.workspace, 'participant', k, attrs)
            assert r.status_code==200
        print('\n    Finished updating participants in {}/{}'.format(self.namespace, self.workspace))


    def update_entity_attributes(self, etype, ename, attr_dict):
        """
        Set or update attributes in attr_dict
        """
        attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
        r = firecloud.api.update_entity(self.namespace, self.workspace, etype, ename, attrs)
        assert r.status_code==200


    def update_sample_attributes(self, sample_id, attr_dict):
        """
        Set or update attributes in attr_dict
        """
        self.update_entity_attributes('sample', sample_id, attr_dict)


    def update_sample_set_attributes(self, sample_set_id, attr_dict):
        """
        Set or update attributes in attr_dict
        """
        self.update_entity_attributes('sample_set', sample_set_id, attr_dict)


    def delete_entity_attributes(self, etype, ename, attrs, check=True):
        """
        Delete attributes
        """
        if isinstance(attrs, str):
            rm_list = [{"op": "RemoveAttribute", "attributeName": attrs}]
        elif isinstance(attrs, Iterable):
            rm_list = [{"op": "RemoveAttribute", "attributeName": i} for i in attrs]
        r = firecloud.api.update_entity(self.namespace, self.workspace, etype, ename, rm_list)
        if check:
            assert r.status_code==200
        else:
            return r


    def delete_sample_set_attributes(self, sample_set_id, attrs):
        """
        Delete attributes
        """
        self.delete_entity_attributes(self, 'sample_set', sample_set_id, attrs)


    def update_attributes(self, attr_dict):
        """
        Set or update workspace attributes. Wrapper for API 'set' call
        """
        attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
        r = firecloud.api.update_workspace_attributes(self.namespace, self.workspace, attrs)  # attrs must be list
        assert r.status_code==200
        print('Successfully updated workspace attributes in {}/{}'.format(self.namespace, self.workspace))


    def get_attributes(self):
        """
        Get workspace attributes
        """
        r = firecloud.api.get_workspace(self.namespace, self.workspace)
        assert r.status_code==200
        attr = r.json()['workspace']['attributes']
        for k in [k for k in attr if 'library:' in k]:
            attr.pop(k)
        return attr


    def get_submission_status(self, filter_active=False, configuration=None, show_namespaces=False):
        """
        Get status of all submissions in the workspace (replicates UI Monitor)
        """
        submissions = firecloud.api.list_submissions(self.namespace, self.workspace)
        assert submissions.status_code==200
        submissions = submissions.json()

        if configuration is not None:
            submissions = [s for s in submissions if configuration in s['methodConfigurationName']]

        statuses = ['Succeeded', 'Running', 'Failed', 'Aborted', 'Submitted', 'Queued']
        df = []
        for s in submissions:
            d = {
                'entity_id':s['submissionEntity']['entityName'],
                'status':s['status'],
                'submission_id':s['submissionId'],
                'date':iso8601.parse_date(s['submissionDate']).strftime('%H:%M:%S %m/%d/%Y'),
            }
            d.update({i:s['workflowStatuses'].get(i,0) for i in statuses})
            if show_namespaces:
                d['configuration'] = s['methodConfigurationNamespace']+'/'+s['methodConfigurationName']
            else:
                d['configuration'] = s['methodConfigurationName']
            df.append(d)
        df = pd.DataFrame(df)
        df.set_index('entity_id', inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df = df[['configuration', 'status']+statuses+['date', 'submission_id']]
        if filter_active:
            df = df[(df['Running']!=0) | (df['Submitted']!=0)]
        return df.sort_values('date')[::-1]


    def get_sample_status(self, configuration):
        """
        Get status of lastest submission for samples in the workspace

        Columns: status (Suceeded, Failed, Submitted), submission timestamp, submission ID
        """

        # get submissions
        submissions = firecloud.api.list_submissions(self.namespace, self.workspace)
        assert submissions.status_code==200
        submissions = submissions.json()

        # filter by configuration
        submissions = [s for s in submissions if configuration in s['methodConfigurationName']]

        # get status of last run submission
        sample_dict = {}
        for s in submissions:
            r = firecloud.api.get_submission(self.namespace, self.workspace, s['submissionId'])
            assert r.status_code==200
            r = r.json()

            ts = datetime.timestamp(iso8601.parse_date(s['submissionDate']))
            if s['submissionEntity']['entityType']=='sample':
                sample_id = s['submissionEntity']['entityName']
                if sample_id not in sample_dict or sample_dict[sample_id]['timestamp']<ts:
                    status = r['workflows'][0]['status']
                    sample_dict[sample_id] = {'status':status, 'timestamp':ts, 'submission_id':s['submissionId'], 'configuration':s['methodConfigurationName'], 'workflow_id':r['workflows'][0]['workflowId']}
            elif s['submissionEntity']['entityType']=='sample_set':
                if len(r['workflows'])==1:
                    sample_id = s['submissionEntity']['entityName']
                    if sample_id not in sample_dict or sample_dict[sample_id]['timestamp']<ts:
                        status = r['workflows'][0]['status']
                        sample_dict[sample_id] = {'status':status, 'timestamp':ts, 'submission_id':s['submissionId'], 'configuration':s['methodConfigurationName'], 'workflow_id':r['workflows'][0]['workflowId']}
                else:
                    for w in r['workflows']:
                        sample_id = w['workflowEntity']['entityName']
                        if sample_id not in sample_dict or sample_dict[sample_id]['timestamp']<ts:
                            if 'workflowId' in w:
                                sample_dict[sample_id] = {'status':w['status'], 'timestamp':ts, 'submission_id':s['submissionId'], 'configuration':s['methodConfigurationName'], 'workflow_id':w['workflowId']}
                            else:
                                sample_dict[sample_id] = {'status':w['status'], 'timestamp':ts, 'submission_id':s['submissionId'], 'configuration':s['methodConfigurationName'], 'workflow_id':'NA'}
            elif s['submissionEntity']['entityType']=='participant':
                participant_id = s['submissionEntity']['entityName']
                if participant_id not in sample_dict or sample_dict[participant_id]['timestamp']<ts:
                    status = r['workflows'][0]['status']
                    sample_dict[participant_id] = {'status':status, 'timestamp':ts, 'submission_id':s['submissionId'], 'configuration':s['methodConfigurationName'], 'workflow_id':r['workflows'][0]['workflowId']}

        status_df = pd.DataFrame(sample_dict).T
        status_df.index.name = 'sample_id'

        # print(status_df['status'].value_counts())
        return status_df[['status', 'timestamp', 'workflow_id', 'submission_id', 'configuration']]


    def get_sample_set_status(self, configuration):
        """
        Get status of lastest submission for samples in the workspace

        Columns: status (Suceeded, Failed, Submitted), submission timestamp, submission ID
        """
        # get submissions
        submissions = firecloud.api.list_submissions(self.namespace, self.workspace)
        assert submissions.status_code==200
        submissions = submissions.json()

        # filter by configuration
        submissions = [s for s in submissions if configuration in s['methodConfigurationName']]

        # get status of last run submission
        sample_set_dict = {}
        for k,s in enumerate(submissions):
            print('\rFetching submission {}/{}'.format(k+1, len(submissions)), end='')
            r = firecloud.api.get_submission(self.namespace, self.workspace, s['submissionId'])
            assert r.status_code==200
            r = r.json()

            ts = datetime.timestamp(iso8601.parse_date(s['submissionDate']))
            if s['submissionEntity']['entityType']=='sample_set':
                if len(r['workflows'])==1:
                    sample_set_id = s['submissionEntity']['entityName']
                    if sample_set_id not in sample_set_dict or sample_set_dict[sample_set_id]['timestamp']<ts:
                        status = r['workflows'][0]['status']
                        sample_set_dict[sample_set_id] = {'status':status, 'timestamp':ts, 'submission_id':s['submissionId'], 'configuration':s['methodConfigurationName'], 'workflow_id':r['workflows'][0]['workflowId']}
                else:
                    for w in r['workflows']:
                        assert w['workflowEntity']['entityType']=='sample_set'
                        sample_set_id = w['workflowEntity']['entityName']
                        if sample_set_id not in sample_set_dict or sample_set_dict[sample_set_id]['timestamp']<ts:
                            status = w['status']
                            sample_set_dict[sample_set_id] = {'status':status, 'timestamp':ts, 'submission_id':s['submissionId'], 'configuration':s['methodConfigurationName'], 'workflow_id':w['workflowId']}
            else:
                raise ValueError('Incompatible submission entity type: {}'.format(s['submissionEntity']['entityType']))
        print()
        status_df = pd.DataFrame(sample_set_dict).T
        status_df.index.name = 'sample_set_id'

        # print(status_df['status'].value_counts())
        return status_df[['status', 'timestamp', 'workflow_id', 'submission_id', 'configuration']]


    def patch_attributes(self, cnamespace, configuration, dry_run=False, entity='sample'):
        """
        Patch attributes for all samples/tasks that run successfully but were not written to database
        This includes outputs from successful tasks in workflows that failed
        """

        # get list of expected outputs
        r = firecloud.api.get_workspace_config(self.namespace, self.workspace, cnamespace, configuration)
        assert r.status_code==200
        r = r.json()
        output_map = {i.split('.')[-1]:j.split('this.')[-1] for i,j in r['outputs'].items()}
        columns = list(output_map.values())

        if entity=='sample':
            # get list of all samples in workspace
            print('Fetching sample status ...')
            samples_df = self.get_samples()
            if len(np.intersect1d(columns, samples_df.columns))>0:
                incomplete_df = samples_df[samples_df[columns].isnull().any(axis=1)]
            else:
                incomplete_df = pd.DataFrame(index=samples_df.index, columns=columns)

            # get workflow status for all submissions
            sample_status_df = self.get_sample_status(configuration)

            # make sure successful workflows were all written to database
            error_ix = incomplete_df.loc[sample_status_df.loc[incomplete_df.index, 'status']=='Succeeded'].index
            if np.any(error_ix):
                print('Attributes from {} successful jobs were not written to database.'.format(len(error_ix)))

            # for remainder, assume that if attributes exists, status is successful.
            # this doesn't work when multiple successful runs of the same task exist --> need to add this

            # for incomplete samples, go through submissions and assign outputs of completed tasks
            task_counts = defaultdict(int)
            for n,sample_id in enumerate(incomplete_df.index):
                print('\rPatching attributes for sample {}/{}'.format(n+1, incomplete_df.shape[0]), end='')

                metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace, sample_status_df.loc[sample_id, 'submission_id'], sample_status_df.loc[sample_id, 'workflow_id'])
                # assert metadata.status_code==200
                # metadata = metadata.json()
                if metadata.status_code==200:
                    metadata = metadata.json()
                    if 'outputs' in metadata and len(metadata['outputs'])!=0 and not dry_run:
                        attr = {output_map[k.split('.')[-1]]:t for k,t in metadata['outputs'].items()}
                        self.update_sample_attributes(sample_id, attr)
                    else:
                        for task in metadata['calls']:
                            if 'outputs' in metadata['calls'][task][-1]:
                                if np.all([k in output_map for k in metadata['calls'][task][-1]['outputs'].keys()]):
                                    # only update if attributes are empty
                                    if incomplete_df.loc[sample_id, [output_map[k] for k in metadata['calls'][task][-1]['outputs']]].isnull().any():
                                        # write to attributes
                                        if not dry_run:
                                            attr = {output_map[i]:j for i,j in metadata['calls'][task][-1]['outputs'].items()}
                                            self.update_sample_attributes(sample_id, attr)
                                        task_counts[task.split('.')[-1]] += 1
                else:
                    print('Metadata call failed for sample {}'.format(sample_id))
                    print(metadata.json())
            print()
            for i,j in task_counts.items():
                print('Samples patched for "{}": {}'.format(i,j))

        elif entity=='sample_set':
            print('Fetching sample set status ...')
            sample_set_df = self.get_sample_sets()
            # get workflow status for all submissions
            sample_set_status_df = self.get_sample_set_status(configuration)

            # any sample sets with empty attributes for configuration
            incomplete_df = sample_set_df.loc[sample_set_status_df.index, columns]
            incomplete_df = incomplete_df[incomplete_df.isnull().any(axis=1)]

            # sample sets with successful jobs
            error_ix = incomplete_df[sample_set_status_df.loc[incomplete_df.index, 'status']=='Succeeded'].index
            if np.any(error_ix):
                print('Attributes from {} successful jobs were not written to database.'.format(len(error_ix)))
                print('Patching attributes with outputs from latest successful run.')
                for n,sample_set_id in enumerate(incomplete_df.index):
                    print('\r  * Patching sample set {}/{}'.format(n+1, incomplete_df.shape[0]), end='')
                    metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace, sample_set_status_df.loc[sample_set_id, 'submission_id'], sample_set_status_df.loc[sample_set_id, 'workflow_id'])
                    assert metadata.status_code==200
                    metadata = metadata.json()
                    if 'outputs' in metadata and len(metadata['outputs'])!=0 and not dry_run:
                        attr = {output_map[k.split('.')[-1]]:t for k,t in metadata['outputs'].items()}
                        self.update_sample_set_attributes(sample_set_id, attr)
                print()
        print('Completed patching {} attributes in {}/{}'.format(entity, self.namespace, self.workspace))


    def display_status(self, configuration, entity='sample', filter_active=True):
        """
        Display summary of task statuses
        """
        # workflow status for each sample (from latest/current run)
        status_df = self.get_sample_status(configuration)

        # get workflow details from 1st submission
        metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace, status_df['submission_id'][0], status_df['workflow_id'][0])
        assert metadata.status_code==200
        metadata = metadata.json()

        workflow_tasks = list(metadata['calls'].keys())

        print(status_df['status'].value_counts())
        if filter_active:
            ix = status_df[status_df['status']!='Succeeded'].index
        else:
            ix = status_df.index

        state_df = pd.DataFrame(0, index=ix, columns=workflow_tasks)
        for k,i in enumerate(ix):
            print('\rFetching metadata for sample {}/{}'.format(k+1, len(ix)), end='')
            metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace, status_df.loc[i, 'submission_id'], status_df.loc[i, 'workflow_id'])
            assert metadata.status_code==200
            metadata = metadata.json()
            state_df.loc[i] = [metadata['calls'][t][-1]['executionStatus'] if t in metadata['calls'] else 'Waiting' for t in workflow_tasks]
        print()
        state_df.rename(columns={i:i.split('.')[1] for i in state_df.columns}, inplace=True)
        summary_df = pd.concat([state_df[c].value_counts() for c in state_df], axis=1).fillna(0).astype(int)
        print(summary_df)
        state_df[['workflow_id', 'submission_id']] = status_df.loc[ix, ['workflow_id', 'submission_id']]

        return state_df, summary_df


    def get_stderr(self, state_df, task_name):
        """
        Fetch stderrs from bucket (returns list of str)
        """
        df = state_df[state_df[task_name]==-1]
        fail_idx = df.index
        stderrs = []
        for n,i in enumerate(fail_idx):
            print('\rFetching stderr for task {}/{}'.format(n+1, len(fail_idx)), end='\r')
            metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace, state_df.loc[i, 'submission_id'], state_df.loc[i, 'workflow_id'])
            assert metadata.status_code==200
            metadata = metadata.json()
            stderr_path = metadata['calls'][[i for i in metadata['calls'].keys() if i.split('.')[1]==task_name][0]][-1]['stderr']
            s = subprocess.check_output('gsutil cat '+stderr_path, shell=True, executable='/bin/bash').decode()
            stderrs.append(s)
        return stderrs


    def get_submission_history(self, configuration, sample_id):
        """
        Currently only supports samples
        """

        # get submissions
        submissions = firecloud.api.list_submissions(self.namespace, self.workspace)
        assert submissions.status_code==200
        submissions = submissions.json()

        # filter by configuration
        submissions = [s for s in submissions if configuration in s['methodConfigurationName']]

        # filter by sample
        submissions = [s for s in submissions if s['submissionEntity']['entityName']==sample_id and 'Succeeded' in list(s['workflowStatuses'].keys())]

        outputs_df = []
        for s in submissions:
            r = firecloud.api.get_submission(self.namespace, self.workspace, s['submissionId'])
            assert r.status_code==200
            r = r.json()

            metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace, s['submissionId'], r['workflows'][0]['workflowId'])
            assert metadata.status_code==200
            metadata = metadata.json()

            outputs_s = pd.Series(metadata['outputs'])
            outputs_s.index = [i.split('.',1)[1].replace('.','_') for i in outputs_s.index]
            outputs_s['submission_date'] = iso8601.parse_date(s['submissionDate']).strftime('%H:%M:%S %m/%d/%Y')
            outputs_df.append(outputs_s)

        outputs_df = pd.concat(outputs_df, axis=1).T
        # sort by most recent first
        outputs_df = outputs_df.iloc[np.argsort([datetime.timestamp(iso8601.parse_date(s['submissionDate'])) for s in submissions])[::-1]]
        outputs_df.index = ['run_{}'.format(str(i)) for i in np.arange(outputs_df.shape[0],0,-1)]

        return outputs_df


    def get_storage(self):
        """
        Get total amount of storage used, in TB

        Pricing: $0.026/GB/month (multi-regional)
                 $0.02/GB/month (regional)
        """
        r = firecloud.api.get_workspace(self.namespace, self.workspace)
        assert r.status_code==200
        r = r.json()
        s = subprocess.check_output('gsutil du -s gs://'+r['workspace']['bucketName'], shell=True, executable='/bin/bash')
        return np.float64(s.decode().split()[0])/1024**4


    def get_stats(self, status_df, workflow_name=None):
        """
        For a list of submissions, calculate time, preemptions, etc
        """
        # for successful jobs, get metadata and count attempts
        status_df = status_df[status_df['status']=='Succeeded'].copy()
        metadata_dict = {}

        @parallelize
        def fetch_workflow_metadata(data):
            k, (i, row) = data
            metadata = firecloud.api.get_workflow_metadata(self.namespace, self.workspace, row['submission_id'], row['workflow_id'])
            assert metadata.status_code == 200
            return (k, i, metadata.json())

        for (k, i, data) in fetch_workflow_metadata(enumerate(status_df.iterrows())):
            print('\rFetching metadata {}/{}'.format(k+1,status_df.shape[0]), end='')
            metadata_dict[i] = data
        print('\r')

        # if workflow_name is None:
            # split output by workflow
        workflows = np.array([metadata_dict[k]['workflowName'] for k in metadata_dict])
        # else:
            # workflows = np.array([workflow_name])

        # get tasks for each workflow
        for w in np.unique(workflows):
            workflow_status_df = status_df[workflows==w]
            tasks = np.sort(list(metadata_dict[workflow_status_df.index[0]]['calls'].keys()))
            task_names = [t.rsplit('.')[-1] for t in tasks]

            task_dfs = {}
            for t in tasks:
                task_name = t.rsplit('.')[-1]
                task_dfs[task_name] = pd.DataFrame(index=workflow_status_df.index, columns=['time_h', 'total_time_h', 'max_preempt_time_h', 'machine_type', 'attempts', 'start_time', 'est_cost', 'job_ids'])
                for i in workflow_status_df.index:
                    successes = {}
                    preemptions = []

                    if 'shardIndex' in metadata_dict[i]['calls'][t][0]:
                        scatter = True
                        for j in metadata_dict[i]['calls'][t]:
                            if j['shardIndex'] in successes:
                                preemptions.append(j)
                            successes[j['shardIndex']] = j  # last shard (assume success follows preemptions)
                    else:
                        scatter = False
                        successes[0] = metadata_dict[i]['calls'][t][-1]
                        preemptions = metadata_dict[i]['calls'][t][:-1]

                    task_dfs[task_name].loc[i, 'time_h'] = np.sum([workflow_time(j)/3600 for j in successes.values()])

                    # subtract time spent waiting for quota
                    quota_time = [e for m in successes.values() for e in m['executionEvents'] if e['description']=='waiting for quota']
                    quota_time = [(convert_time(q['endTime']) - convert_time(q['startTime']))/3600 for q in quota_time]
                    task_dfs[task_name].loc[i, 'time_h'] -= np.sum(quota_time)

                    total_time_h = [workflow_time(t_attempt)/3600 for t_attempt in metadata_dict[i]['calls'][t]]
                    task_dfs[task_name].loc[i, 'total_time_h'] = np.sum(total_time_h) - np.sum(quota_time)

                    if not np.any(['hit' in j['callCaching'] and j['callCaching']['hit'] for j in metadata_dict[i]['calls'][t]]):
                        was_preemptible = [j['preemptible'] for j in metadata_dict[i]['calls'][t]]
                        if len(preemptions)>0:
                            assert was_preemptible[0]
                            task_dfs[task_name].loc[i, 'max_preempt_time_h'] = np.max([workflow_time(t_attempt) for t_attempt in preemptions])/3600
                        task_dfs[task_name].loc[i, 'attempts'] = len(metadata_dict[i]['calls'][t])

                        task_dfs[task_name].loc[i, 'start_time'] = iso8601.parse_date(metadata_dict[i]['calls'][t][0]['start']).astimezone(pytz.timezone(self.timezone)).strftime('%H:%M')

                        machine_types = [j['jes']['machineType'].rsplit('/')[-1] for j in metadata_dict[i]['calls'][t]]
                        task_dfs[task_name].loc[i, 'machine_type'] = machine_types[-1]  # use last instance

                        task_dfs[task_name].loc[i, 'est_cost'] = np.sum([get_vm_cost(m,p)*h for h,m,p in zip(total_time_h, machine_types, was_preemptible)])

                        task_dfs[task_name].loc[i, 'job_ids'] = ','.join([j['jobId'] for j in successes.values()])

            # add overall cost
            workflow_status_df['est_cost'] = pd.concat([task_dfs[t.rsplit('.')[-1]]['est_cost'] for t in tasks], axis=1).sum(axis=1)
            workflow_status_df['time_h'] = [workflow_time(metadata_dict[i])/3600 for i in workflow_status_df.index]
            workflow_status_df['cpu_hours'] = pd.concat([task_dfs[t.rsplit('.')[-1]]['total_time_h'] * task_dfs[t.rsplit('.')[-1]]['machine_type'].apply(lambda i: int(i.rsplit('-',1)[-1]) if ('-small' not in i and '-micro' not in i) else 1) for t in tasks], axis=1).sum(axis=1)
            workflow_status_df['start_time'] = [iso8601.parse_date(metadata_dict[i]['start']).astimezone(pytz.timezone(self.timezone)).strftime('%H:%M') for i in workflow_status_df.index]

        return workflow_status_df, task_dfs


    def publish_config(self, from_cnamespace, from_config, to_cnamespace, to_config, public=False):
        """
        copy configuration to repository
        """
        # check whether prior version exists
        r = get_config(to_cnamespace, to_config)
        old_version = None
        if r:
            old_version = np.max([m['snapshotId'] for m in r])
            print('Configuration {}/{} exists. SnapshotID: {}'.format(
                to_cnamespace, to_config, old_version))

        # copy config to repo
        r = firecloud.api.copy_config_to_repo(self.namespace, self.workspace, from_cnamespace, from_config, to_cnamespace, to_config)
        assert r.status_code==200
        print("Successfully copied {}/{}. New SnapshotID: {}".format(to_cnamespace, to_config, r.json()['snapshotId']))

        # make configuration public
        if public:
            print('  * setting public read access.')
            r = firecloud.api.update_repository_config_acl(to_cnamespace, to_config, r.json()['snapshotId'], [{'role': 'READER', 'user': 'public'}])

        # delete old version
        if old_version is not None:
            r = firecloud.api.delete_repository_config(to_cnamespace, to_config, old_version)
            assert r.status_code==200
            print("Successfully deleted SnapshotID {}.".format(old_version))


    def import_config(self, cnamespace, cname):
        """
        Import configuration from repository
        """
        # get latest snapshot
        c = get_config(cnamespace, cname)
        c = c[np.argmax([i['snapshotId'] for i in c])]
        r = firecloud.api.copy_config_from_repo(self.namespace, self.workspace, cnamespace, cname, c['snapshotId'], cnamespace, cname)
        if r.status_code==201:
            print('Successfully imported configuration {} (SnapshotId {})'.format(cname, c['snapshotId']))
        else:
            print(r.text)


    def get_entities(self, etype, page_size=1000):
        """
        Paginated query replacing 'get_entities_tsv'
        """
        # get first page
        r = firecloud.api.get_entities_query(self.namespace, self.workspace, etype, page=1, page_size=page_size)
        assert r.status_code==200
        r = r.json()

        total_pages = r['resultMetadata']['filteredPageCount']
        all_entities = r['results']
        for page in range(2,total_pages+1):
            r = firecloud.api.get_entities_query(self.namespace, self.workspace, etype, page=page, page_size=page_size)
            assert r.status_code==200
            r = r.json()
            all_entities.extend(r['results'])

        # convert to DataFrame
        df = pd.DataFrame({i['name']:i['attributes'] for i in all_entities}).T
        df.index.name = etype+'_id'
        return df


    def get_samples(self):
        """
        Get DataFrame with samples and their attributes
        """
        # r = firecloud.api.get_entities_tsv(self.namespace, self.workspace, 'sample')
        # assert r.status_code==200
        # return pd.read_csv(io.StringIO(r.text), index_col=0, sep='\t')
        df = self.get_entities('sample')
        df['participant'] = df['participant'].apply(lambda x: x['entityName'])
        return df


    def get_sample_sets(self, pattern=None):
        """
        Get DataFrame with sample sets and their attributes
        """
        r = firecloud.api.get_entities(self.namespace, self.workspace, 'sample_set')
        assert r.status_code==200
        r = r.json()

        if pattern is not None:
            r =[i for i in r if pattern in i['name']]

        sample_set_ids = [i['name'] for i in r]
        columns = np.unique([k for s in r for k in s['attributes'].keys()])
        df = pd.DataFrame(index=sample_set_ids, columns=columns)
        for s in r:
            for c in columns:
                if c in s['attributes']:
                    if isinstance(s['attributes'][c], dict):
                        df.loc[s['name'], c] = [i['entityName'] if 'entityName' in i else i for i in s['attributes'][c]['items']]
                    else:
                        df.loc[s['name'], c] = s['attributes'][c]
        return df


    def get_participants(self):
        """
        Get DataFrame with participants and their attributes
        """
        df = self.get_entities('participant')
        return df


    def update_sample_set(self, sample_set_id, sample_ids):
        """
        Update (or create) a sample set
        """
        r = firecloud.api.get_entity(self.namespace, self.workspace, 'sample_set', sample_set_id)
        if r.status_code==200:  # exists -> update
            r = r.json()
            items_dict = r['attributes']['samples']
            items_dict['items'] = [{'entityName': i, 'entityType': 'sample'} for i in sample_ids]
            attrs = [{'addUpdateAttribute': items_dict, 'attributeName': 'samples', 'op': 'AddUpdateAttribute'}]
            r = firecloud.api.update_entity(self.namespace, self.workspace, 'sample_set', sample_set_id, attrs)
            assert r.status_code==200
            print('Sample set "{}" ({} samples) successfully updated.'.format(sample_set_id, len(sample_ids)))
        else:  # create
            set_df = pd.DataFrame(data=np.c_[[sample_set_id]*len(sample_ids), sample_ids], columns=['membership:sample_set_id', 'sample_id'])
            buf = io.StringIO()
            set_df.to_csv(buf, sep='\t', index=False)
            r = firecloud.api.upload_entities(self.namespace, self.workspace, buf.getvalue())
            buf.close()
            assert r.status_code==200
            print('Sample set "{}" ({} samples) successfully created.'.format(sample_set_id, len(sample_ids)))


    def update_super_set(self, super_set_id, sample_set_ids, sample_ids):
        """
        Update (or create) a set of sample sets

        Defines the attribute "sample_sets_"

        sample_ids: at least one 'dummy' sample is needed
        """
        if isinstance(sample_ids, str):
            self.update_sample_set(super_set_id, [sample_ids])
        else:
            self.update_sample_set(super_set_id, sample_ids)

        attr_dict = {
            "sample_sets_": {
                "itemsType": "EntityReference",
                "items": [{"entityType": "sample_set", "entityName": i} for i in sample_set_ids]
            }
        }
        attrs = [firecloud.api._attr_set(i,j) for i,j in attr_dict.items()]
        r = firecloud.api.update_entity(self.namespace, self.workspace, 'sample_set', super_set_id, attrs)
        if r.status_code==200:
            print('Set of sample sets "{}" successfully created.'.format(super_set_id))
        else:
            print(r.text)


    def delete_sample_set(self, sample_set_id):
        """
        Delete sample set
        """
        r = firecloud.api.delete_sample_set(self.namespace, self.workspace, sample_set_id)
        assert r.status_code==204
        print('Sample set "{}" successfully deleted.'.format(sample_set_id))


    def find_sample_set(self, sample_id, sample_set_df=None):
        if sample_set_df is None:
            sample_set_df = self.get_sample_sets()
        return [i for i,s in zip(sample_set_df.index, sample_set_df['samples']) if 'GTEX-1C2JI-0626-SM-7DHM1' in s]


    def purge_outdated(self, attribute, bucket_files=None, samples_df=None, ext=None):
        """
        Delete outdated files matching attribute (e.g., from prior/outdated runs)
        """
        if bucket_files is None:
            bucket_files = gs_list_bucket_files(self.get_bucket_id())

        if samples_df is None:
            samples_df = self.get_samples()

        try:
            assert attribute in samples_df.columns
        except:
            raise ValueError('Sample attribute "{}" does not exist'.format(attribute))

        # make sure all samples have attribute set
        assert samples_df[attribute].isnull().sum()==0

        if ext is None:
            ext = np.unique([os.path.split(i)[1].split('.',1)[1] for i in samples_df[attribute]])
            assert len(ext)==1
            ext = ext[0]

        purge_paths = [i for i in bucket_files if i.endswith(ext) and i not in set(samples_df[attribute])]
        if len(purge_paths)==0:
            print('No outdated files to purge.')
        else:
            bucket_id = self.get_bucket_id()
            assert np.all([i.startswith('gs://'+bucket_id) for i in purge_paths])

            while True:
                s = input('{} outdated files found. Delete? [y/n] '.format(len(purge_paths))).lower()
                if s=='n' or s=='y':
                    break

            if s=='y':
                print('Purging {} outdated files.'.format(len(purge_paths)))
                gs_delete(purge_paths, chunk_size=500)


    def _par_delete_sample_attribute(self, args):
        """
        Wrapper for parallelized calls to delete_entity_attributes
        """
        sample_id = args[0]
        attribute = args[1]
        return sample_id, self.delete_entity_attributes('sample', sample_id, attribute, check=False)


    def delete_entity_attributes(self, delete_s, etype, delete_files=False):
        """
        Delete sample attributes and their associated data

        delete_s: pd.Series with sample_id -> path; delete_s.name is attribute to delete
        """
        op = [{'attributeName':delete_s.name, 'op':'RemoveAttribute'}]
        attrs = [{'name':i, 'entityType':etype, 'operations': op} for i in delete_s.index]
        r = firecloud.api.batch_update_entities(self.namespace, self.workspace, attrs)
        if r.status_code==204:
            print("Successfully deleted attribute '{}' for {} samples.".format(delete_s.name, len(delete_s)))
        else:
            print(r.text)

        if delete_files:
            print('Deleting files')
            gs_delete(delete_s)


    def update_configuration(self, json_body):
        """
        Create or update a method configuration (separate API calls)

        json_body = {
           'namespace': config_namespace,
           'name': config_name,
           'rootEntityType' : entity,
           'methodRepoMethod': {'methodName':method_name, 'methodNamespace':method_namespace, 'methodVersion':version},
           'methodNamespace': method_namespace,
           'methodConfigVersion':1,
           'inputs':  {},
           'outputs': {},
           'prerequisites': {},
           'deleted': False
        }

        """
        r = firecloud.api.list_workspace_configs(self.namespace, self.workspace)
        if json_body['name'] not in [m['name'] for m in r.json()]:
            # configuration doesn't exist -> name, namespace specified in json_body
            r = firecloud.api.create_workspace_config(self.namespace, self.workspace, json_body)
            assert r.status_code==201
            print('Successfully added configuration: {}'.format(json_body['name']))
        else:
            r = firecloud.api.update_workspace_config(self.namespace, self.workspace, json_body['namespace'], json_body['name'], json_body)
            assert r.status_code==200
            print('Successfully updated configuration: {}'.format(json_body['name']))
        return r


    def check_configuration(self, config_name):
        """
        Get version of a configuration and compare to latest available in repository
        """
        r = firecloud.api.list_workspace_configs(self.namespace, self.workspace)
        assert r.status_code==200
        r = r.json()
        r = [i for i in r if i['name']==config_name][0]
        # method repo version
        mrversion = get_method_version(r['methodRepoMethod']['methodNamespace'], r['methodRepoMethod']['methodName'])
        print('Method for config. {0}: {1} version {2} (latest: {3})'.format(config_name, r['methodRepoMethod']['methodName'], r['methodRepoMethod']['methodVersion'], mrversion))
        return r['methodRepoMethod']['methodVersion']


    def get_configs(self, latest_only=False):
        """
        All configurations in the workspace
        """
        r = firecloud.api.list_workspace_configs(self.namespace, self.workspace)
        assert r.status_code==200
        r = r.json()
        df = pd.io.json.json_normalize(r)
        df.rename(columns={c:c.split('methodRepoMethod.')[-1] for c in df.columns}, inplace=True)
        if latest_only:
            df = df.sort_values(['methodName','methodVersion'], ascending=False).groupby('methodName').head(1)
            # .sort_values('methodName')
            # reverse sort
            return df[::-1]
        return df

    def create_submission(self, cnamespace, config, entity, etype, expression=None, use_callcache=True):
        """

        """
        r = firecloud.api.create_submission(self.namespace, self.workspace,
            cnamespace, config, entity, etype, expression=expression, use_callcache=use_callcache)
        if r.status_code==201:
            print('Successfully created submission {}.'.format(r.json()['submissionId']))
        else:
            print(r.text)


#------------------------------------------------------------------------------
# Functions for parsing Google metadata
#------------------------------------------------------------------------------
def get_google_metadata(job_id):
    """
    jobid: operations ID
    """
    if isinstance(job_id, str):
        s = subprocess.check_output('gcloud alpha genomics operations describe '+job_id+' --format json', shell=True, executable='/bin/bash')
        return json.loads(s.decode())
    elif isinstance(job_id, Iterable):
        json_list = []
        for k,j in enumerate(job_id):
            print('\rFetching metadata ({}/{})'.format(k+1,len(job_id)), end='')
            s = subprocess.check_output('gcloud alpha genomics operations describe '+j+' --format json', shell=True, executable='/bin/bash')
            json_list.append(json.loads(s.decode()))
        return json_list


def parse_google_stats(json_list):
    """
    Parse job start and end times, machine type, and preemption status from Google metadata
    """
    df = pd.DataFrame(index=[j['name'] for j in json_list], columns=['time_h', 'machine_type', 'preemptible', 'preempted'])
    for j in json_list:
        event_dict = {k['description']:convert_time(k['startTime']) for k in j['metadata']['events'] if 'copied' not in k}
        event_times = [convert_time(k['startTime']) for k in j['metadata']['events'] if 'copied' not in k]
        time_delta = np.max(event_times) - np.min(event_times)
        # if 'ok' in event_dict:
        #     time_delta = convert_time(event_dict['ok']) - convert_time(event_dict['start'])
        # elif 'start-shutdown' in event_dict:
        #     time_delta = convert_time(event_dict['start-shutdown']) - convert_time(event_dict['start'])
        # else:
        #     raise ValueError('unknown event')
        mt = j['metadata']['runtimeMetadata']['computeEngine']['machineType'].split('/')[-1]
        p = j['metadata']['request']['ephemeralPipeline']['resources']['preemptible']
        # j[0]['metadata']['request']['pipelineArgs']['resources']['preemptible']
        df.loc[j['name'], ['time_h', 'machine_type', 'preemptible', 'preempted']] = [time_delta/3600, mt, p, 'ok' not in event_dict]
    return df


def calculate_google_cost(jobid, jobid_lookup_df):
    """
    Calculate cost
    """
    r = jobid_lookup_df.loc[jobid]
    if r['preempted'] and r['time_h']<1/6:
        return 0
    else:
        return r['time_h']*get_vm_cost(r['machine_type'], preemptible=r['preemptible'])


#------------------------------------------------------------------------------
# Functions for managing methods and configuration in the repository
#------------------------------------------------------------------------------
def list_methods(namespace=None):
    """
    List all methods in the repository
    """
    r = firecloud.api.list_repository_methods()
    assert r.status_code==200
    r = r.json()

    if namespace is not None:
        r = [m for m in r if m['namespace']==namespace]

    return pd.DataFrame(r).sort_values(['name', 'snapshotId'])


def get_method(namespace, name):
    """
    Get all available versions of a method from the repository
    """
    r = firecloud.api.list_repository_methods()
    assert r.status_code==200
    r = r.json()
    r = [m for m in r if m['name']==name and m['namespace']==namespace]
    return r


def get_method_version(namespace, name):
    """
    Get latest method version
    """
    r = get_method(namespace, name)
    return np.max([m['snapshotId'] for m in r])


def list_configs(namespace=None):
    """
    List all configurations in the repository
    """
    r = firecloud.api.list_repository_configs()
    assert r.status_code==200
    r = r.json()

    if namespace is not None:
        r = [m for m in r if m['namespace']==namespace]

    return pd.DataFrame(r).sort_values(['name', 'snapshotId'])


def get_config(namespace, name):
    """
    Get all versions of a configuration from the repository
    """
    r = firecloud.api.list_repository_configs()
    assert r.status_code==200
    r = r.json()
    r = [m for m in r if m['name']==name and m['namespace']==namespace]
    return r


def get_config_version(namespace, name):
    """
    Get latest config version
    """
    r = get_config(namespace, name)
    return np.max([m['snapshotId'] for m in r])


def print_methods(namespace):
    """
    Print all methods in a namespace
    """
    r = firecloud.api.list_repository_methods()
    assert r.status_code==200
    r = r.json()
    r = [m for m in r if m['namespace']==namespace]
    methods = np.unique([m['name'] for m in r])
    for k in methods:
        print('{}: {}'.format(k, np.max([m['snapshotId'] for m in r if m['name']==k])))


def print_configs(namespace):
    """
    Print all configurations in a namespace
    """
    r = firecloud.api.list_repository_configs()
    assert r.status_code==200
    r = r.json()
    r = [m for m in r if m['namespace']==namespace]
    configs = np.unique([m['name'] for m in r])
    for k in configs:
        print('{}: {}'.format(k, np.max([m['snapshotId'] for m in r if m['name']==k])))


def get_wdl(method_namespace, method_name, snapshot_id=None):
    """
    Get WDL from repository
    """
    if snapshot_id is None:
        snapshot_id = get_method_version(namespace, name)

    r = firecloud.api.get_repository_method(method_namespace, method_name, snapshot_id)
    assert r.status_code==200
    return r.json()['payload']


def compare_wdls(mnamespace1, mname1, mnamespace2, mname2):
    """
    Compare WDLs from two methods
    """
    v1 = get_method_version(mnamespace1, mname1)
    v2 = get_method_version(mnamespace2, mname2)
    wdl1 = get_wdl(mnamespace1, mname1, v1)
    wdl2 = get_wdl(mnamespace2, mname2, v2)
    print('Comparing:')
    print('< {}:{}.v{}'.format(mnamespace1, mname1, v1))
    print('> {}:{}.v{}'.format(mnamespace2, mname2, v2))
    cmd = 'diff <(echo \''+wdl1+'\') <(echo \''+wdl2+'\')'
    d = subprocess.run(cmd, shell=True, executable='/bin/bash', stdout=subprocess.PIPE)
    print(d.stdout.decode())


def compare_wdl(mnamespace, mname, wdl_path):
    """
    Compare method WDL to file
    """
    v = get_method_version(mnamespace, mname)
    wdl1 = get_wdl(mnamespace, mname, v)
    with open(wdl_path) as f:
        wdl2 = f.read()
    print('Comparing:')
    print('< {}'.format(wdl_path))
    print('> {}:{}.v{}'.format(mnamespace, mname, v))
    cmd = 'diff <(echo \''+wdl1+'\') <(echo \''+wdl2+'\')'
    d = subprocess.run(cmd, shell=True, executable='/bin/bash', stdout=subprocess.PIPE)
    print(d.stdout.decode())

def redact_outdated_method_versions(method_namespace, method_name):
    """

    """
    r = firecloud.api.list_repository_methods()
    assert r.status_code==200
    r = r.json()
    r = [m for m in r if m['name']==method_name and m['namespace']==method_namespace]
    versions = np.array([m['snapshotId'] for m in r])
    print('Latest version: {}'.format(np.max(versions)))
    versions = versions[versions!=np.max(versions)]
    for i in versions:
        print('  * deleting version {}'.format(i))
        r = firecloud.api.delete_repository_method(method_namespace, method_name, i)
        assert r.status_code==200


def update_method(namespace, method, synopsis, wdl_file, public=False, delete_old=True):
    """
    push new version, then redact previous version(s)
    """
    # check whether prior version exists
    r = get_method(namespace, method)
    old_version = None
    if r:
        old_version = np.max([m['snapshotId'] for m in r])
        print('Method {}/{} exists. SnapshotID: {}'.format(
            namespace, method, old_version))

    # push new version
    r = firecloud.api.update_repository_method(namespace, method, synopsis, wdl_file)
    if r.status_code==201:
        print("Successfully pushed {}/{}. New SnapshotID: {}".format(namespace, method, r.json()['snapshotId']))
    else:
        print(r.text)

    if public:
        print('  * setting public read access.')
        r = firecloud.api.update_repository_method_acl(namespace, method, r.json()['snapshotId'], [{'role': 'READER', 'user': 'public'}])

    # delete old version
    if old_version is not None and delete_old:
        r = firecloud.api.delete_repository_method(namespace, method, old_version)
        assert r.status_code==200
        print("Successfully deleted SnapshotID {}.".format(old_version))


#------------------------------------------------------------------------------
# VM costs
#------------------------------------------------------------------------------
def get_vm_cost(machine_type, preemptible=True):
    """
    Cost per hour
    """
    preemptible_dict = {
        'n1-standard-1': 0.0100,  # 3.75 GB
        'n1-standard-2': 0.0200,  # 7.5 GB
        'n1-standard-4': 0.0400,  # 15  GB
        'n1-standard-8': 0.0800,  # 30  GB
        'n1-standard-16':0.1600,  # 60  GB
        'n1-standard-32':0.3200,  # 120 GB
        'n1-standard-64':0.6400,  # 240 GB
        'n1-highmem-2':  0.0250,  # 13  GB
        'n1-highmem-4':  0.0500,  # 26  GB
        'n1-highmem-8':  0.1000,  # 52  GB
        'n1-highmem-16': 0.2000,  # 104 GB
        'n1-highmem-32': 0.4000,  # 208 GB
        'n1-highmem-64': 0.8000,  # 416 GB
        'n1-highcpu-2':  0.0150,  # 1.80 GB
        'n1-highcpu-4':  0.0300,  # 3.60 GB
        'n1-highcpu-8':  0.0600,  # 7.20 GB
        'n1-highcpu-16': 0.1200,  # 14.40 GB
        'n1-highcpu-32': 0.2400,  # 28.80 GB
        'n1-highcpu-64': 0.4800,  # 57.6 GB
        'f1-micro':      0.0035,  # 0.6 GB
        'g1-small':      0.0070,  # 1.7 GB
    }

    standard_dict = {
        'n1-standard-1': 0.0475,
        'n1-standard-2': 0.0950,
        'n1-standard-4': 0.1900,
        'n1-standard-8': 0.3800,
        'n1-standard-16': 0.7600,
        'n1-standard-32': 1.5200,
        'n1-standard-64': 3.0400,
        'n1-highmem-2':  0.1184,
        'n1-highmem-4':  0.2368,
        'n1-highmem-8':  0.4736,
        'n1-highmem-16': 0.9472,
        'n1-highmem-32': 1.8944,
        'n1-highmem-64': 3.7888,
        'n1-highcpu-2':  0.0709,
        'n1-highcpu-4':  0.1418,
        'n1-highcpu-8':  0.2836,
        'n1-highcpu-16': 0.5672,
        'n1-highcpu-32': 1.1344,
        'n1-highcpu-64': 2.2688,
        'f1-micro':      0.0076,
        'g1-small':      0.0257,
    }

    if preemptible:
        return preemptible_dict[machine_type]
    else:
        return standard_dict[machine_type]

def main(argv=None):
    if not argv:
        argv = sys.argv

    # Initialize core parser
    descrip  = 'dalmatian [OPTIONS] CMD [arg ...]\n'
    descrip += '       dalmatian [ --help | -v | --version ]'
    parser = argparse.ArgumentParser(description='dalmatian: the loyal companion to FISS. Only currently useful in a REPL.')

    parser.add_argument("-v", "--version", action='version', version=__version__)

    args = parser.parse_args()

    sys.exit()

if __name__ == '__main__':
    sys.exit(main())
