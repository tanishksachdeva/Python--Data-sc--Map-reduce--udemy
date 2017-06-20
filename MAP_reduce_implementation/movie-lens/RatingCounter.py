from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self,key ,line):# def amapper func to input a complete line
        (userID , movieID ,rating ,timestamp) = line.split('\t') # all this is seperated by tab , therefore split tab
        yield rating, 1 # we need rating , throw away everything yield only rating -generating a key value pair , rating: nos of occur
    
# input this to a reducer function soreted and grouped values
    
    def reducer(self , rating , occurences):
        yield rating , sum(occurences) #o/p is just the rating value , and summing up all the occurences
        
# every mapreduce will have this to run , the class name may change though
if __name__=='__main__':
    MRRatingCounter.run()
    
    
# !python RatingCounter.py ml-100k\u.data
# use this to run the code , python file name.py and the dataset directory