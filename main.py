""" Script to download events from Sentry
    and output into csv file
"""
import os
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


# Create dict from list of event tags
def flatten_event_tags(tags):
    flatten_tags = {}
    for tag in tags:
        if (tag.get('key')):
            flatten_tags[tag.get('key')] = tag.get('value')
    return flatten_tags


def clean_data(response_data):
    df = pd.DataFrame(response_data)
    df['tags'] = df['tags'].apply(flatten_event_tags)

    # Extract userId from user object
    df['userId'] = df['user'].apply(lambda u: u['id'])

    # Extract relevant tags fields into their own columns
    # Note: df['tags'].apply(pd.Series) won't work because some events have garbage fields
    tags_df = pd.json_normalize(df['tags'])
    tag_fields = ['accountId', 'clientId',
                  'environment', 'pageId', 'release', 'userAgent']
    combined_df = pd.concat([df, tags_df[tag_fields]], axis=1)

    return combined_df


def filter_production_events(df):
    mask = df['environment'] == 'production'
    return df[mask]


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

    # issue_ids = sentry.fetch_issues(PROJECT_NAME)
    issue_ids = mock_data.issue_ids  # MOCK

    print(f'Found {len(issue_ids)} issues in {PROJECT_NAME} sentry project')

    for issue_id in issue_ids:
        # fetch all issue's events
        response = sentry.fetch_events(issue_id)
        # parse json response into tabular format
        events = transform(response)
        # write to CSV file
        file_name = os.path.join('./data', f'event_{issue_id}.csv')
        events.to_csv(file_name, index=False)
        print(f'{file_name} written successfully.')

    print(f'Completed!')
