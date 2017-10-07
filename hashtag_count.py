import os
os.environ['S3_USE_SIGV4'] = 'True'

from mrjob.job import MRJob

class HashtagCount(MRJob):
    def map(self, _, tweet):
        """
        Output the number of hashtags and a count for each tweet
        the '_' can be used when there is no key
        """
        num_hashtags = sum(1  for i in tweet.split() if i.startswith("#"))
        yield 'tweets', 1
        yield 'hashtags', num_hashtags

    def reduce(self, key, value):
        # a simple sum function
        yield key, sum(value)

    def steps(self):
        """
        the steps can be modified to compose any number of map/reduce steps
        by including multiple instances of self.mr
        """
        return [self.mr(mapper=self.map,
                        combiner=self.reduce,
                        reducer=self.reduce)]

if __name__ == '__main__':
    HashtagCount.run()
del os.environ['S3_USE_SIGV4']
