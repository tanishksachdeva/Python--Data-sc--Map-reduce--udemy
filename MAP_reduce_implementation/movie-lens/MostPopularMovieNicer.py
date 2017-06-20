from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    
    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='C:\Users\Tanishk\ml-100k\u.item')# this file found will be distributed to every node, anciallry data will be passed to every node it ll be needed at
   
    def steps(self):
        #reducer_init will run before we even lookup 
        return [
            MRStep(mapper=self.mapper_rat,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_cnt),
            MRStep(reducer = self.reducer_max)
        ]
             
    def mapper_rat(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_init(self):
        self.movieNames = {}
        
        with open("C:\Users\Tanishk\ml-100k\u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]
                
    def reducer_cnt(self, key, values):
        yield None, (sum(values), self.movieNames[key])
                
    def reducer_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
