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
import json
import sentry

# MOCK DATA
import mock_data

# Fetch events from Sentry. Returns list of JSON objects
# def fetch_events_by_project(project_name):
#     # MOCK read from local file for now:
#     with open('/Users/sharonli/scripts/20200428_sentry_events/mock/Get_events_by_project-sentry-loop2-res.json') as res_file:
#         return json.loads(res_file.read())


def is_production_event(event):
    tag_list = event.get('tags')
    environment = next(
        tag for tag in tag_list if tag.get('key') == 'environment')
    return environment.get('value') == 'production'


# Create dict from list of event tags
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

    # append fields from tags (Loop 2 tags)
    event['account_id'] = tag_dict.get('accountId')
    event['client_id'] = tag_dict.get('clientId')
    event['page_id'] = tag_dict.get('pageId')
    event['release'] = tag_dict.get('release')
    event['user_agent'] = tag_dict.get('userAgent')
    return event


def write_to_csv(events, filename):
    with open(filename, mode='w') as output_file:
        output_writer = csv.writer(output_file)

        # Write header
        output_writer.writerow(['event_id', 'timestamp', 'issue_id', 'issue_name',
                                'user_id', 'account_id', 'client_id', 'page_id', 'release', 'user_agent'])
        # Write rows
        for event in events:
            output_writer.writerow(event.values())


if (__name__ == '__main__'):
    PROJECT_NAME = 'mi-loops-2'

    # issue_ids = sentry.fetch_issues(PROJECT_NAME)
    issue_ids = mock_data.issue_ids # MOCK

    print(f'Found {len(issue_ids)} issues in {PROJECT_NAME} sentry project')

  
    # Testing out single issue
    events = sentry.fetch_events('1613488552')
    # filter out non-production events
    prod_events = list(filter(is_production_event, events))
    # transform json data into csv row
    rows = list(map(transform, prod_events))
    print(f'Writing {len(rows)} production events to csv...')
    write_to_csv(rows, filename = f'event-1613488552.csv')

    print('Completed')
