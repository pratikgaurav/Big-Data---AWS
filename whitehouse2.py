import os
os.environ['S3_USE_SIGV4'] = 'True'

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import operator
import io

output=[]
seen = set()
l = 0
class MRMostUsedWord(MRJob):
    def mapper_get_words(self,_,tweet):
        with open('/home/ubuntu/mrjobs/WhiteHouse-WAVES-2012.csv', encoding="latin-1") as cvsfile:
            reader = csv.reader(cvsfile)
            if l == 0:
                for i,row in enumerate(reader):
                    global l
                    l = 1
                    yield '%s %s' % (row[19],row[20]), 1
                    


    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        output.append((sum(counts),word))
        output.sort(key=lambda r:r[0],reverse = True)

    def reducer_find_Top_Visitors(self):
        for i,result in enumerate(output):
            if i<=9:
                yield result
           
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),MRStep(reducer_final=self.reducer_find_Top_Visitors)
        ]


if __name__ == '__main__':
    MRMostUsedWord.run()

del os.environ['S3_USE_SIGV4']
