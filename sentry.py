import requests
import os
import re
import time

SENTRY_HOST = 'https://sentry.io/api/0'


def fetch_issues(project_name):
    url = f'{SENTRY_HOST}/projects/unbounce/{project_name}/issues/'
    issues = _make_get_request(url)

    # Extract list of issue IDs
    return list(map(lambda i: i.get('id'), issues))


def fetch_events(issue_id):
    url = f'{SENTRY_HOST}/issues/{issue_id}/events/'
    events = _make_get_request(url)
    return events


"""
Make GET request to Sentry
Recursive function to poll all paginated data
"""


def _make_get_request(url, entries=[]):
    res = requests.get(
        url, headers={"Authorization": f"Bearer {os.getenv('SENTRY_API_KEY')}"})

    if res.status_code == 200:
        # concat new entries to existing
        next_entries = entries + res.json()

        # check if paginated
        next_page_url = _check_next_pagination(res.headers.get('Link'))
        if next_page_url:
            # throttle subsequent requests
            time.sleep(1)
            return _make_get_request(next_page_url, next_entries)
        else:
            print(f'Found {len(next_entries)} total entries.')
            return next_entries
    else:
        print(
            f"Cannot fetch issues by project: Received {res.status_code} status code.")
        return None


# Check if next request is required. If so, returns url
def _check_next_pagination(link):
    next_pagination = link.split(', ')[1].split('; ')
    # Extract pagination info
    next_url = re.search(r"<(.*)>", next_pagination[0]).group(1)
    next_result = re.search(r"results=\"(true|false)\"",
                            next_pagination[2]).group(1)

    if (next_result == 'true'):
        return next_url
    else:
        return False
