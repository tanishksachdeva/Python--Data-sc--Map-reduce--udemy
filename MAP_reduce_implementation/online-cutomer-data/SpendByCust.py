from mrjob.job import MRJob
from mrjob.step import MRStep

import mrjob.protocol

from decimal import Decimal
# in this we are actualy calc the amount a customer spend in the shopping 
class MRCustomerTotalExpenses(MRJob):
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.ReprProtocol
# two map reduce func are used first to process what we need and second to sort the results
# the first one calc the results and inouts its data in the seconnd one.
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.mapper_sorter, reducer=self.reducer_swap),
        ]

    def mapper(self, key, line):
        customerID, itemID, amount = line.split(',')
        yield customerID, Decimal(amount)

    def reducer(self, customerID, amounts):
        yield customerID, sum(amounts)

    def mapper_sorter(self, customerID, total):
        yield "{:9.2f}".format(total), customerID # used to place in correct format

    def reducer_swap(self, total, customers):
        for customer in customers:
            yield customer, total

if __name__ == "__main__":
    MRCustomerTotalExpenses.run()
    
#!python SpendByCust.py customer-orders.csv
# use this to run the file