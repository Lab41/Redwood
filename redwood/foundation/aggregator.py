#!/usr/bin/env python
#
# Copyright (c) 2013 In-Q-Tel, Inc/Lab41, All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Created on 19 October 2013
@author: Lab41
"""


class Aggregator():

    def __init__(self, cnx):
        self.cnx = cnx


    def aggregate(self, filter_list, dist_str=None):
        '''
        should come in as a:x, b:y, c:z, etc, where x+y+z = 100, and a-c are filter ids
        standard aggregate is equally weighted
        '''

        weights = list()
        #TODO: make the dup_list a dict
        dup_list = list()

        if not dist_str is None:
            if len(dist_str) != len(filter_list):
                print "The number of loaded filters ({}) does not equal the number of provided weights ({})".format(len(filter_list), len(dist_str))
                return
            try:
                for s in dist_str:
                    p = s.split(':')
                    filter_id = int(p[0])
                    percent = float(p[1])

                    if filter_id in dup_list:
                        print "Error: Mutliple weights entered for filter with id {}".format(filter_id)
                        return
                    dup_list.append(filter_id)
                    weights.append((filter_id, percent / float(100)))

                if sum([w[1] for w in weights]) != 1:
                    print "The filter weights must total 100"
                    return
            except:
                print "There was an error with your sytax, try again"
                return
        else:
            i = 0
            even_split = 1 / float(len(filter_list))
            for f in filter_list:
                weights.append((i, even_split))
                i += 1

        query = "UPDATE unique_file\n"

        #now create the query

        for w in weights:
            fltr = filter_list[w[0]]
            print "{} weight -> {}".format(fltr.name, w[1])
            query += "LEFT JOIN " + fltr.score_table + " ON " + fltr.score_table + ".id = unique_file.id\n"

        query += "SET unique_file.reputation = ("

        for filter_id, weight in weights:
            fltr = filter_list[filter_id]
            query += "{} * {}.score + ".format(weight, fltr.score_table)

        #remove the last +
        query = query[0:len(query)-3]
        query += ")"
        cursor = self.cnx.cursor()
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()
