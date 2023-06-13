import argparse
import sys 
import os 
import pandas as pd
import datetime
import re
from typing import List,Dict
from github import Github
import aiohttp
import asyncio

def convert_time(
        ts: str
    ):
    '''
    Converts the timestamps that are strings to datetime.strptime object 
    '''
    if not ts:
        return None
    return datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ')

def get_pr_lifetime(
        created_at,
        merged_at,
        closed_at
    ) -> datetime.timedelta:
    """
    return the PR lifetime in days. 
    """
    if merged_at:
        lifetime = merged_at - created_at 
        return lifetime.days 
    #merged issued will be closed, but closed issues are not merged 
    if closed_at and not merged_at:
        lifetime = closed_at - created_at
        return lifetime.days
    # if it's still open, calculate today into the lifetime
    else:
        lifetime = datetime.datetime.now() - created_at
        return lifetime.days
    


def process_prs(repo: str,
                prs: List ,
                stats :List ,
                start_date : datetime.datetime,
                end_date: datetime.datetime,
                gh: Github
    ) -> List[Dict]: 

    for pr_list in prs['data']: 
        for pr in pr_list:
            created_at = convert_time(pr.get("created_at"))
            merged_at = convert_time(pr.get("merged_at"))
            closed_at = convert_time(pr.get("closed_at"))

            # Verify that the PR fits within the scope
            if start_date <= created_at <= end_date:

                pr_number = pr.get('number')
                params = {'per_page': 100}
                pr = gh.get_pr(repo,pr_number, params)['data'][0]
                lifetime = get_pr_lifetime(created_at,merged_at,closed_at) 
                engineer = pr.get('user').get('login')
                pr_state = pr.get('state')
                comments = pr.get('comments')
                commits = pr.get('commits')
                additions = pr.get('additions')
                deletions = pr.get('deletions')
                changed_files = pr.get('changed_files')

                row = {
                    'repo' : repo, 
                    'engineer': engineer,
                    'pr_number': pr_number,
                    'created_at': created_at.strftime("%Y-%m-%d"),
                    'pr_state': pr_state,
                    'pr_lifetime_days':  lifetime,
                    'commits' : commits,
                    'comments' : comments, 
                    'additions': additions, 
                    'deletions' : deletions,
                    'changed_files' : changed_files
                }
                stats.append(row)

            else: 
                continue

    return stats

async def main(args):
    # define args from the CLI 
    org = args.org
    excluded_repos = args.excludedRepos
    verbose = args.verbose
    start_date = convert_time(args.startDate)
    end_date = convert_time(args.endDate)
    target_branch = args.targetBranch

    token = os.environ['GITHUB_ACCESS_TOKEN']
    g = Github(token, org)

    params = {
        'per_page': 100
    
    }
    
    repos = await g.get_repo_list(params) 

    if not excluded_repos:
        excluded_repos = []

    stats = []
    for repo in repos: 

        if verbose:

            print(f"repo: {repo}")

        if repo not in excluded_repos:

            params = {
                'state' : "all", 
                'per_page': 100, 
                'base': target_branch 
            }
            
            #Process the PRs 
            prs = g.get_pr_list(repo, params)   
            stats =  process_prs(repo,prs,stats,start_date, end_date,g) 

    df = pd.DataFrame.from_records(stats)
    df.to_csv('out.csv')
                      
                
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument( 
        '-t', 
        '--targetBranch',
        type=str, 
        required=True,
        help="this is the branch that reflects a production deploymen. Typically 'main'"
    ) 

    parser.add_argument( 
        '-rs', 
        '--refString',
        type=str, 
        required=False,
        help="string to search for in a refering branch. If using gitflow, 'release' might be the string to match by "
    ) 

    parser.add_argument( 
        '-e', 
        '--excludedRepos',
        type=str, 
        required=False,
        action='append',
        help="list of repos to exclude "
    ) 

    parser.add_argument( 
        '-md', 
        '--maxDays',
        type=int, 
        required=False,
        help="maximum number of days to search (from now)"
    ) 

    parser.add_argument( 
        '-o', 
        '--org',
        type=str, 
        required=True,
        help="name of the organization in github"
    ) 

    parser.add_argument( 
        '-r', 
        '--repo',
        type=str, 
        required=False,
        help="Specify a specifc repo. Default is all in the org."
    ) 

    parser.add_argument( 
        '-v', 
        '--verbose',
        type=bool, 
        required=False,
        help="Print out more details"
    ) 

    parser.add_argument( 
        '-rm', 
        '--resultMethod',
        type=str, 
        required=False,
        help="Options are percentile[0-9][0-9] or mean.  Example --rm percentile90  "
    ) 

    parser.add_argument( 
        '-sd', 
        '--startDate',
        type=str, 
        required=True,
        help=""
    ) 

    parser.add_argument( 
        '-ed', 
        '--endDate',
        type=str, 
        required=True,
        help=""
    ) 

    args = parser.parse_args()
    asyncio.run(main(args))

