import argparse
import datetime

import boto3
import pytz

BATCH_IMAGE_DELETE_ITEM = 100


class EcrRepo():
    def __init__(self, name, ecr_client):
        self._name = name
        self._ecr_client = ecr_client
        self._all_images = list()
        self._expired_images = list()
        
    def get_name(self):
        return self._name
    
    def get_images(self):
        images = list()
        next_token = ''
        while True:
            if next_token:
                result = self._ecr_client.describe_images(repositoryName=self._name, filter={'tagStatus': 'ANY'},
                                                          nextToken=next_token)
            else:
                result = self._ecr_client.describe_images(repositoryName=self._name, filter={'tagStatus': 'ANY'})
        
            for image in result['imageDetails']:
                images.append(image)
        
            if 'nextToken' not in result:
                break
            else:
                next_token = result['nextToken']

        images.sort(key=lambda x: x['imagePushedAt'], reverse=True)
        self._all_images = images
        return images
    
    def get_expired_images(self, versions_to_keep, days_to_keep):
        utc = pytz.UTC
        datetime_to_keep = utc.localize(datetime.datetime.utcnow() - datetime.timedelta(days=days_to_keep))
        
        if len(self._all_images) < 1:
            self.get_images()
        
        if len(self._all_images) <= versions_to_keep:
            return list()
        
        expired_images_candidate = self._all_images[versions_to_keep:]

        expired_images = [image for image in expired_images_candidate if image['imagePushedAt'] < datetime_to_keep]
        self._expired_images = expired_images
        return expired_images
    
    def delete_expired_images(self, versions_to_keep, days_to_keep):
        if len(self._expired_images) < 1:
            self.get_expired_images(versions_to_keep, days_to_keep)

        if len(self._expired_images) < 1:
            print(f'No image fit remove rule from {self._name} repo.', flush=True)
            return
        
        print(f'Removing {len(self._expired_images)} image(s) from {self._name} repo.', flush=True)
        expired_image_ids = [{'imageDigest': i['imageDigest']} for i in self._expired_images]
        while len(expired_image_ids) > 1:
            # batch_delete_image have limit for 100 images
            if len(expired_image_ids) > BATCH_IMAGE_DELETE_ITEM:
                x = BATCH_IMAGE_DELETE_ITEM
                self._ecr_client.batch_delete_image(repositoryName=self._name, imageIds=expired_image_ids[:x])
                expired_image_ids = expired_image_ids[x:]
            else:
                self._ecr_client.batch_delete_image(repositoryName=self._name, imageIds=expired_image_ids)
                # Or just use break is better??
                expired_image_ids = list()
    
    @classmethod
    def get_repos(cls, ecr_client):
        repos = list()
        next_token = ''
        while True:
            if next_token:
                result = ecr_client.describe_repositories(maxResults=10, nextToken=next_token)
            else:
                result = ecr_client.describe_repositories(maxResults=10)
        
            for repo in result['repositories']:
                if repo['repositoryName'] in skip_repo:
                    continue
                repos.append(EcrRepo(repo['repositoryName'], ecr_client))
        
            if 'nextToken' not in result:
                break
            else:
                next_token = result['nextToken']
        return repos


def setup_argparse():
    parser = argparse.ArgumentParser(
        description='Remove ECR image(s), just keep number latest push and/or images pushed in last x days.')
    parser.add_argument('--keep-latest', type=int, default=30, help='Number of latest push image to keep')
    parser.add_argument('--keep-day', type=int, default=90, help='Number of days to keep')
    parser.add_argument('-s', '--skip-repo', action='append', help='Repo want to skip.')
    return parser.parse_args()

    
if __name__ == '__main__':
    args = setup_argparse()

    new_version_to_keep = args.keep_latest
    day_to_keep = args.keep_day
    skip_repo = args.skip_repo
    if skip_repo is None:
        skip_repo = list()
    
    ecr_client = boto3.client('ecr')
    
    repos = EcrRepo.get_repos(ecr_client)
    
    for r in repos:
        r.delete_expired_images(new_version_to_keep, day_to_keep)
