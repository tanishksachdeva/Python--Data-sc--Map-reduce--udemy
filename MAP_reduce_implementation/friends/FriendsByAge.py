from mrjob.job import MRJob

class MRFriendsByAge(MRJob):
# we need to find out the avg nos of friends for each age
    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',') # using comma as delimiter instead of tab as it is a  comma seperated file
        yield age, float(numFriends) # this will yileds age with every occurence of friend

    def reducer(self, age, numFriends):
        total = 0 # here finding the total nos of age , passed argumennts age and friends
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
            
        yield age, total / numElements


if __name__ == '__main__':
    MRFriendsByAge.run()
    
#!python FriendsByAge.py fakefriends.csv
# use this for o/p