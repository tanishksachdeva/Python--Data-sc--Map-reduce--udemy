from mrjob.job import MRJob

class userwiserating(MRJob):
    def mapper(self,key,line):
        (userID,movieID,rating,timestamp) = line.split('\t')
        yield userID,movieID
    def reducer(self,userID,movielist):
        movie=[]
        for m in movielist:
            movie.append(m)
        yield userID,len(movie)

if __name__ == '__main__':
    userwiserating.run()