""" Script to download events from Sentry
    and output into csv file

    Columns:
    event_id ('id')
    timestamp ('dateCreated')
    issue_id ('groupID')
    issue_title ('title')
    user_uuid (user.id)
    account_uuid (tags > key = 'accountId')
    client_uuid (tags > key = 'clientId')
    page_uuid (tags > key = 'pageId')
    release (tags > key = 'release')
    user_agent (tags > key = 'userAgent')
    environment (filter only 'production')
"""
import csv
import requests
import json

# SENTRY.PY


def fetch_events_by_project(project_name):
    with open('/Users/sharonli/scripts/20200428_sentry_events/mock/Get_events_by_project-sentry-loop2-res.json') as res_file:
        return json.loads(res_file.read())  # a list
# SENTRY.PY


def is_production_event(event):
    tag_list = event.get('tags')
    environment = next(  # read on iterators
        tag for tag in tag_list if tag.get('key') == 'environment')  # check syntax
    return environment.get('value') == 'production'


def write_to_csv(events):
    with open('output.csv', mode='w') as output_file:
        output_writer = csv.writer(output_file)

        # Write header
        output_writer.writerow(['event_id', 'timestamp', 'issue_id', 'issue_name',
                                'user_id', 'account_id', 'client_id', 'page_id', 'release', 'user_agent'])

        for event in events:
            output_writer.writerow(event.values())


def flatten_event_tags(tags):
    flatten_tags = {}
    for tag in tags:
        if (tag.get('key')):
            flatten_tags[tag.get('key')] = tag.get('value')
    return flatten_tags


def transform(e):
    event = {
        'event_id': e.get('id'),
        'timestamp': e.get('dateCreated'),
        'issue_id': e.get('groupID'),
        'issue_name': e.get('message'),
        'user_id': e.get('user', {}).get('id'),
    }
    # flatten event tag list
    tag_dict = flatten_event_tags(e.get('tags', []))

    # append fields from tags
    event['account_id'] = tag_dict.get('accountId')
    event['client_id'] = tag_dict.get('clientId')
    event['page_id'] = tag_dict.get('pageId')
    event['release'] = tag_dict.get('release')
    event['user_agent'] = tag_dict.get('userAgent')
    return event


if (__name__ == '__main__'):
    PROJECT_NAME = 'mi-loops-2'

    # fetch events from Sentry by project name
    json_response = fetch_events_by_project(PROJECT_NAME)

    # filter out non-production events
    sentry_events = list(filter(is_production_event, json_response))
    events = list(map(transform, sentry_events))

    print(f'Writing {len(events)} production events to csv...')

    write_to_csv(events)
