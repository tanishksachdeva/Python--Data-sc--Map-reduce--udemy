from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):#to figure out the most pop movie by most rated
    def steps(self):
        return [
            MRStep(mapper=self.mapper_rat,
                   reducer=self.reducer_cnt),
            MRStep(reducer = self.reducer_max)
        ]
             
    def mapper_rat(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1 # yields what we need movie id and ,1 fo everytime it occurs

    def reducer_cnt(self, key, values):
        yield None, (sum(values), key) # just returns the sum i.e complete list sum and sorted

    def reducer_max(self, key, values): #runs on the vals from previous reducer will be caleed ony once unlike the other mapreduce pair above and returns only 1 most pop movie
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
    
#!python  MostPopularMovie.py ml-100k/u.data
# run using this
