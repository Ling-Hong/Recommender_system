"""
Created on 2018-11-30 17:21:27
@author: Lynn.H
@description: 
@reference:

http://aimotion.blogspot.com/2012/08/introduction-to-recommendations-with.html

https://datastat.io/posts/2017/Jan/15/Movie-Recommendations-Part-1/

"""

import math
import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations


class MR_program(MRJob):

    def configure_args(self):
        super(MR_program, self).configure_args()
        self.add_passthru_arg('-m')

    # def pearson(self, x, y):
    #     """
    #     :param x: a list of scores for movie 1
    #     :param y: a list of scores for movie 2
    #     :return: pearson coefficient
    #     """
    #     n = len(x) # or len(y)
    #     sumx = sum(x)
    #     sumy = sum(y)
    #     sumxy = sum([i * j for i, j in zip(x, y)])
    #     sumx2 = sum([i**2 for i in x])
    #     sumy2 = sum([j**2 for j in y])
    #     numerator = n*sumxy - sumx*sumy
    #     a = n*sumx2 - sumx**2
    #     b = n*sumy2 - sumy**2
    #     denominator = (a*b)**1/2
    #     if denominator==0:
    #         return (x,y)
    #     else:
    #         return numerator/denominator


    def pearson(self, s1, s2):

        n = len(s1)

        # Sums of all the preferences
        sumX = sum(s1)
        sumY = sum(s2)

        # Sums of the squares
        sumXSq = sum([pow(i, 2) for i in s1])
        sumYSq = sum([pow(i, 2) for i in s2])

        # Sum of the products
        sumXY = sum([i * j for i, j in zip(s1, s2)])

        # Calculate r (Pearson score)
        num = sumXY - (sumX * sumY / n)
        den = math.sqrt((sumXSq - pow(sumX, 2) / n) * (sumYSq - pow(sumY, 2) / n))
        if den == 0: return 0

        r = num / den

        return r

    def mapper(self, _, line):
        user_id, movie_id, rating, _ = line.split(',')
        try:
            yield user_id, (movie_id, float(rating))
        except:
            pass

    def reducer_1(self, _, values):
        current_movie = self.options.m
        for movie1, movie2 in combinations(values,2):
            if movie1[0]==current_movie:
                yield (movie1[0],movie2[0]),(movie1[1],movie2[1])
            elif movie2[0]==current_movie:
                yield (movie2[0],movie1[0]),(movie2[1],movie1[1])

    def reducer_2(self, key, values):
        movie1, movie2 = key
        score1 = []
        score2 = []
        for i in values:
            s1, s2 = i
            score1.append(s1)
            score2.append(s2)
        pearson_val = self.pearson(score1, score2)
        yield movie1, (movie2, pearson_val)

    def reducer_3(self, key, values):
        all_movies = list(values)
        # sort
        # input is movie1:(movie2,0.7)
        result = sorted(all_movies, key=lambda x:x[1], reverse=True)
        yield key, result


    def steps(self):
        return [MRStep(mapper=self.mapper)] + \
               [MRStep(reducer=self.reducer_1)] + \
               [MRStep(reducer=self.reducer_2)] + \
               [MRStep(reducer=self.reducer_3)]


if __name__ == '__main__':
    MR_program.run()
