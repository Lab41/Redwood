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

import os
import shutil
import math
from redwood.filters import filter_list
from redwood.helpers import core
import matplotlib.pylab as plt
from redwood.foundation.aggregator import Aggregator


class Report():
    def __init__(self, cnx, source_info):
        self.report_dir = "reports"
        self.cnx = cnx
        self.source = source_info

    def run(self, agg_weights=None):

        print "Running report survey for: " + self.source.source_name
        print "... aggregating most recent filter scores"
        ag = Aggregator(self.cnx)
        ag.aggregate(filter_list, agg_weights)
        self.run_filter_survey()
        self.generate_report()

    #collects survey reports from each filter and aggregates the results into one central report
    def run_filter_survey(self):
        print "...Generating Report"
        for f in filter_list:
            f.cnx = self.cnx
        print f.name
        path = f.run_survey(self.source.source_name)
        try:
            shutil.rmtree(self.report_dir + "/" + self.source.source_name + "/filters/" + f.name)
        except:
            pass

        if path == None:
            continue

        shutil.move(path, self.report_dir + "/" + self.source.source_name + "/filters/" + f.name)

    def generate_report(self):
        report_dir = "reports/" + self.source.source_name
        report_file = self.source.source_name + "_report.html"
        html_file = os.path.join(report_dir, report_file)

        score_counts = core.get_reputation_by_source(self.cnx, self.source.source_name)

        bins = [.05,.1,.15,.2,.25,.30,.35,.40,.45,.50,.55,.60,.65,.70,.75,.80,.85,.90,.95,1.00]
        scores, counts = zip(*score_counts)
        fig = plt.figure()
        ax = fig.add_subplot(111, title="Reputation Distribution")
        ax.hist(scores, weights=counts, bins = bins)
        ax.set_xlabel("Reputation Score")
        ax.set_ylabel("File Occurrences")

        threshold = None
        #TODO: if you have a truth source, use it here
        #if you have a validation engine, use the line below
        #threshold = core.get_malware_reputation_threshold(self.cnx)
        #print "thres: {}".format(threshold)
        #if threshold is not None:
        #    plt.axvline(x=threshold, color="r", ls='--')
        #plt.xticks(bins)

        #for tick in ax.xaxis.get_major_ticks():
        #    tick.label.set_fontsize(8)

        hist_reputation = os.path.join(report_dir, "rep.png")
        plt.savefig(hist_reputation)


        table_height = int(math.ceil(len(score_counts) / float(3)))
        file_count = 0
        for s in score_counts:
            file_count += s[1]

        with open(html_file, 'w') as f:
            f.write("""
            <html>
            <head>
            <link href="../resources/css/style.css" rel="stylesheet" type="text/css">
            </head>
            <body>
            <div id="navigation">
                <image class="center" src="../resources/images/redwood_logo.png" height="25%"/>
                <ul id="navigation" class="list">""")
            for d in os.listdir(report_dir + "/filters"):
                if os.path.isdir(os.path.join(report_dir + "/filters", d)) and d[0] != '.':
                    filter_survey = os.path.join("filters/" + d, "survey.html")
                    f.write("""
                    <li><a class="button" href=\"{}\">{}</a></dt>
            """.format(filter_survey, d))
            f.write("""
                </ul>
            </div>
            <div id="top">
                <h2 class="redwood-title">Report for {}</h2>\n""".format(self.source.source_name))
            f.write("\t\t<h3 class=\"redwood-header\">Source Information</h3>\n")
            f.write("\t\t<dl style=\"text-indent: 5px;\">\n")
            f.write("\t\t\t<dt>Acquisition Date: {}</dt>\n".format(self.source.date_acquired))
            f.write("\t\t\t<dt>Operating System: {}</dt>\n".format(self.source.os_name))
            f.write("\t\t\t<dt>File Count: {}</dt>\n".format(file_count))
            f.write("\t\t</dl>\n\t\t</div>\n")
            f.write("\t\t<div id=\"content\">\n")
            f.write("\t\t<table border=\"1\" id=\"redwood-table\">\n")
            f.write("\t\t\t<caption class=\"caption\">File Score Distribution</caption>\n")
            f.write("\t\t\t<thead></thead>\n")
            f.write("""
                <caption id="redwood-table" class="caption">Reputation Distribution</caption>
                <img src="rep.png"/>
            """)
            f.write("""
            <thead>
                <tr>
                    <th scope="col" class="rounded-head-left">Score</th>
                    <th scope="col" class="count-divider">Count</th>
                    <th scope="col" class="score-divider">Score</th>
                    <th scope="col" class="count-divider">Count</th>
                    <th scope="col" class="score-divider">Score</th>
                    <th scope="col" class="rounded-head-right-light">Count</th>
                </tr>
            </thead>
            <tbody>""")
            for i in range(0, table_height):
                if len(score_counts) == 1:
                    f.write("""
            </tbody>
            <tfoot>
                <tr>
                    <td class="rounded-foot-left">{}</td>
                    <td class="count-divider">{}</td>
                    <td class="score-divider"></td>
                    <td class="count-divider"></td>
                    <td class="score-divider"></td>
                    <td class="rounded-foot-right-light"></td>
                </tr>
            </tfoot>""".format(score_counts[i][0], score_counts[i][1]))
                elif table_height * 2 + i >= len(score_counts):
                    if i == table_height - 1:
                        f.write("""
            </tbody>
            <tfoot>
                <tr>
                    <td class="rounded-foot-left">{}</td>
                    <td class="count-divider">{}</td>
                    <td class="score-divider">{}</td>
                    <td class="count-divider">{}</td>
                    <td class="score-divider"></td>
                    <td class="rounded-foot-right-light"></td>
                </tr>
            </tfoot>""".format(score_counts[i][0], score_counts[i][1],
                score_counts[table_height + i][0], score_counts[table_height + i][1]))
                    else:
                        f.write("""
                <tr>
                    <td class="score-divider">{}</td>
                    <td class="count-divider">{}</td>
                    <td class="score-divider">{}</td>
                    <td class="count-divider">{}</td>
                    <td class="score-divider"></td>
                    <td></td>
                </tr>""".format(score_counts[i][0], score_counts[i][1],
                        score_counts[table_height + i][0], score_counts[table_height + i][1]))
                else:
                    f.write("""
                <tr>
                    <td class="score-divider">{}</td>
                    <td class=count-divider>{}</td>
                    <td class="score-divider">{}</td>
                    <td class=count-divider>{}</td>
                    <td class="score-divider">{}</td>
                    <td>{}</td>
                </tr>""".format(score_counts[i][0], score_counts[i][1],
                    score_counts[table_height + i][0], score_counts[table_height + i][1],
                    score_counts[table_height * 2 + i][0], score_counts[table_height * 2 + i][1]))
            f.write("""
            </table>
            </div>
            """)
            #This is what the query should look like in production
            cursor = self.cnx.cursor()
            #cursor.execute("""
            #    SELECT file_metadata.file_name AS Filename,
            #    unique_file.reputation AS Reputation,
            #    unique_path.full_path As Path,
            #    unique_file.hash AS Hash
            #    FROM file_metadata
            #    INNER JOIN unique_file
            #    ON file_metadata.unique_file_id = unique_file.id
            #    INNER JOIN unique_path
            #    ON file_metadata.unique_path_id = unique_path.id
            #    WHERE source_id = {}
            #    ORDER BY unique_file.reputation ASC
            #    LIMIT 0, 100
            #    """.format(source.os_id))
            #Use this query if unique_file.reputation is no indexed
            cursor.execute("""
                SELECT file_metadata.file_name AS Filename,
                unique_file.reputation AS Reputation,
                unique_path.full_path As Path,
                unique_file.hash AS Hash
                FROM file_metadata
                INNER JOIN unique_file
                ON file_metadata.unique_file_id = unique_file.id
                INNER JOIN unique_path
                ON file_metadata.unique_path_id = unique_path.id
                WHERE source_id = %s
                ORDER BY unique_file.reputation ASC
                LIMIT 0, 100
                """, (self.source.source_id,))
            col_length = len(cursor.description)
            field_names = cursor.description
            results = cursor.fetchall()
            f.write("\t\t<div id=\"content\">\n")
            f.write("\t\t<table border=\"1\" id=\"redwood-table\">\n")
            f.write("\t\t\t<caption class=\"caption\">Lowest Reputation Files (100)</caption>\n")
            f.write("""
            <thead>
                <tr>""")
            for i in range(0, col_length):
                if i == 0:
                    f.write("<th scope=\"col\" class=\"rounded-head-left\">{}</th>".format(field_names[i][0]))
                elif i == col_length - 1:
                    f.write("<th class=\"rounded-head-right\">{}</td>".format(field_names[i][0]))
                else:
                    f.write("<th>{}</th>".format(field_names[i][0]))
            f.write("""
                </tr>
            </thead>
            <tbody>""")
            for row in results:
                f.write("<tr>")
                for l in row:
                    f.write("<td>{}</td>".format(l))
                f.write("</tr>\n")
            f.write("</tbody></table></div>")
            f.write("</body></html>")
            f.close()
