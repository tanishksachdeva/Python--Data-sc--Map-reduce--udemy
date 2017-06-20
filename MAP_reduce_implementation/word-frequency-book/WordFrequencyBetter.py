from mrjob.job import MRJob
import re
# creating a better version to split up words , using regular expressions
# this will not only ignore white spaces in the previous method but also focus on the puctuations


WORD_REGEXP = re.compile(r"[\W']+") 

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors="ignore") #avoids issues in mrjob 5.0
            yield word.lower(), 1# for each word to lower case and count 1

    def reducer(self, key, values):# job is to count the sum of occur
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
    
#!python WordFrequencyBetter.py Book.txt 
# use this to run file