""" Script to download events from Sentry
    and output into csv file
"""
import csv
import json
import pandas as pd
import sentry

# MOCK DATA
import mock_data

FIELD_NAME_MAP = {
    'eventID': 'event_id',
    'dateCreated': 'timestamp',
    'groupID': 'issue_id',
    'title': 'issue_name',
    'userId': 'user_id',
    'accountId': 'account_uuid',
    'clientId': 'client_uuid',
    'pageId': 'page_uuid',
    'release': 'app_version',
    'userAgent': 'user_agent'
}


def fetch_events_by_project(project_name):
    # MOCK read from local file for now:
    with open('/Users/sharonli/scripts/20200428_sentry_events/mock/Get_events_by_project-sentry-loop2-res.json') as res_file:
        return json.loads(res_file.read())


# def is_production_event(event):
#     tag_list = event.get('tags')
#     environment = next(
#         tag for tag in tag_list if tag.get('key') == 'environment')
#     return environment.get('value') == 'production'


# Create dict from list of event tags
def flatten_event_tags(tags):
    flatten_tags = {}
    for tag in tags:
        if (tag.get('key')):
            flatten_tags[tag.get('key')] = tag.get('value')
    return flatten_tags


# def transform(e):
#     event = {
#         'event_id': e.get('id'),
#         'timestamp': e.get('dateCreated'),
#         'issue_id': e.get('groupID'),
#         'issue_name': e.get('message'),
#         'user_id': e.get('user', {}).get('id'),
#     }
#     # flatten event tag list
#     tag_dict = flatten_event_tags(e.get('tags', []))

#     # append fields from tags (Loop 2 tags)
#     event['account_id'] = tag_dict.get('accountId')
#     event['client_id'] = tag_dict.get('clientId')
#     event['page_id'] = tag_dict.get('pageId')
#     event['release'] = tag_dict.get('release')
#     event['user_agent'] = tag_dict.get('userAgent')
#     return event


def clean_data(response_data):
    df = pd.DataFrame(response_data)
    df['tags'] = df['tags'].apply(flatten_event_tags)

    # Extract userId from user object
    df['userId'] = df['user'].apply(lambda u: u['id'])

    # Extract tags fields into their own columns
    tag_fields = ['accountId', 'clientId', 'environment',
                  'level', 'pageId', 'release', 'user', 'userAgent']
    df[tag_fields] = df['tags'].apply(pd.Series)

    return df


def filter_production_events(df):
    mask = df['environment'] == 'production'
    return df[mask]


# def write_to_csv(events, filename):
#     with open(filename, mode='w') as output_file:
#         output_writer = csv.writer(output_file)

#         # Write header
#         output_writer.writerow(['event_id', 'timestamp', 'issue_id', 'issue_name',
#                                 'user_id', 'account_id', 'client_id', 'page_id', 'release', 'user_agent'])
#         # Write rows
#         for event in events:
#             output_writer.writerow(event.values())


def transform(response):
    # flatten JSON data into Dataframe
    cleaned_events = clean_data(response)
    # filter out non-production events
    events = filter_production_events(cleaned_events)

    # select columns for csv
    csv_content = events[['eventID', 'dateCreated', 'groupID', 'title',
                          'userId', 'accountId', 'clientId', 'pageId', 'release', 'userAgent']]

    # rename them to something more comprehensible
    return csv_content.rename(columns=FIELD_NAME_MAP)


if (__name__ == '__main__'):
    PROJECT_NAME = 'mi-loops-2'

    response = fetch_events_by_project(PROJECT_NAME)
    csv = transform(response)

    csv.to_csv('output.csv', index=False)
    # issue_ids = sentry.fetch_issues(PROJECT_NAME)
    # issue_ids = mock_data.issue_ids  # MOCK

    # print(f'Found {len(issue_ids)} issues in {PROJECT_NAME} sentry project')

    # for issue_id in issue_ids:
    #     events = sentry.fetch_events(issue_id)
    #     # filter out non-production events
    #     prod_events = list(filter(is_production_event, events))
    #     # transform json data into csv row
    #     rows = list(map(transform, prod_events))
    #     print(f'Writing {len(rows)} production events to csv...')
    #     write_to_csv(rows, filename=f'event-{issue_id}.csv')

    print('Completed')
