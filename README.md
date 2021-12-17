This repository contains code from an analytic built to analyze the chemistry of a soccer team, chemistry scores between pairs of players on the same team are reported. For a detailed look at the motivation and development of this analytic please see this medium article: https://medium.com/@samiazim_66285/chemistry-in-soccer-4823f9818dfc

Description of analytic:
Here is an overview, the model is built on the following framework, VAEP score, where
VAEP(event_i) = (Pr(score)_event_i - Pr(score)_event_(i-1)) - (Pr(concede)_event_i - Pr(concede)_event_(i-1))

Where Pr(score)_event_i is the probability of scoring on event i. I felt that this framework was too oriented to scoring so I modified it slightly, to get

VAEP_Modified(event_i) = ((Pr(score)_event_i - Pr(score)_event_(i-1)) - (Pr(concede)_event_i - Pr(concede)_event_(i-1))) +
		        ((Pr(keep_possession)_event_i - Pr(keep_possession)_event_(i-1)) - (Pr(lose_possession)_event_i - Pr(lose_possession)_event_(i-1)))

In order to estimate the probabilites, you need to train 4 different classification models on the following labels:
- goal scoring, the event is labeled 1 if there is a goal scored by the team in next k events
- concede, the event is labelled 1 if there is a goal concedes by the team in the next k events
- keep possession, the event is labelled 1 if if the current event is a defensive actions and the team wins the ball in the next k events or if the team already has the ball and holds on to it in the next k events.
- lose possession, the event is labelled 1 if the current event if the opposition team is performing a defensive action and wins the ball in the next k events, or if the opposition team already has ball and keeps it in the next k events

I chose k = 5, random number that makes sense in the domain, less than that and it doesn’t take into account cascading actions that lead to a goal, and more than 5 includes events who may not have a huge influence on goal/possession change.

In order to get the JointVAEP you simply add the VAEP scores if two consecutive actions are performed by players on the same team. To get season VAEP you just add up all the joint scores in each match, and add up the scores in all the matches to get one score for the whole season.


Buld and Run Analytic:
There is no need to build the code, you just run each file on spark shell in this order. See screenshots for more information. Additionally, the scala files are hardcoded with the paths in my HDFS directory, aka “hdfs:///user/sa5064/Project” in terms of reading and writing data.

In order to build the project completely from scratch in a different directory, you need to change the paths. The entire project input and outputs are in the Project directory. You need to put the stats bomb data in Project/Data/, so that the Project/Data/open-data contains the stats bomb data. If you run the project in my directory, then you shouldn’t need to change anything, and the entire project is designed to overwrite files from previous runs.

In the screenshots, I ran the GetPredictions.scala file last since it takes around 4 hours to run completely on the spark-shell, I already had previous predictions saved in HDFS so I used those to run the analytic in the screenshot. However to run it completely in the spark shell from start to finish, please do it in the order below.
GetPaths.scala
CheckPaths.scala
CleanProfileData.scala
PrepForML.scala
GetPredictions.scala
GetModVAEP_GBT.scala
GetModVAEP_LR.scala
Also note, I added comments to the code in the submission however the version that I used to run the analytic don’t have comments.


Description of Data:
open-data/data has three relevent directories: 
open-data/data/matches which contains the match information of each competition for example open-data/data/matches/11/26.json contains the list of matches in the 2014-2015 FC Barcelona La liga Season
open-data/data/lineups/<match_id> contains the lineup for each game, named by <match_id> which can be found from the competition data above
open-data/data/events/<match_id> contains the events for each game, this is the data that I am mostly interested in.

Descrption of each folder and file:

Data Ingest:
This just contains commands on peel server to clone the github repo with the data and put it in the correct HDFS directory.

etl_code:
I was only interested in running this analytic for the 2014-2015 FC Barcelona La Liga season, GetPaths.scala takes the competition file for that season from the source data and generates the list of paths to each match’s events and lineups. CheckPaths.scala makes sure that these paths exists in the data (the github repo specified that there is event data missing sometimes). The match paths are put in a file located at: hdfs:///user/sa5064/Project/Data/matchPaths/MatchPaths.snappy.parquet

profiling_code:
This only contaings CleanProfileData.scala
I selected the columns that I am interested in, which were index, event type, player, team, possession team, location, timestamp, whether it’s a shot, a substitution. I added columns that tell me if a shot results in a goal and the replacement player if it’s a subtitution, and then dropped the original shot/subtitution columns. I also added a literal column with the match id for each row, this is so I can partition by match when I perform operations on the aggregated data for the entire season.
I hard coded the possible entries that each event type can take and made sure each event in the data corresponds to one of these values, and made sure that all of the location/time data makes sense, and player names / teams match with lineups. Given the nature of the data, it it satisfies all of the constrains it’s pretty much impossible to tell if it’s actually correct or not without watching the game, for example if the x and y values of a location are flipped, the data still appears correct.
The clean data for each match is stored in a directory called hdfs:///user/sa5064/Project/Data/FinalData/Clean/ and each match has filename <match_id>.snappy.parquet.

ana_code:
PrepForML.scala: 
If you look at the top of the README, you need to label the data in order to train the classification models, this file generates these labels and drops unwanted rows (for example, rows that indicate the start of a half, or substitution).
The data is stored in a directory called: hdfs:///user/sa5064/Project/Data/FinalData/ReadyForPredictions/ and each file has name <match_id>PREDICT.snappy.parquet.
GetPredictions.scala:
All of the match data for the season is aggregated into one DF, for each label (see top of file for list), a gradient boosted tree model is trained as well as a logistic regression.
The models are saved in hdfs:///user/sa5064/Project/Models/GBT/<label>/ and hdfs:///user/sa5064/Project/Models/LR/<label>/ and the predictions are saved in hdfs:///user/sa5064/Project/Data/FinalData/Predictions/GBT/ and hdfs:///user/sa5064/Project/Data/FinalData/Predictions/LR/ where file names are <label>_GBTPredictions.snappy.parquet or <label>_LRPredictions.snappy.parquet where label can be : Label_Goal,Label_Concede,Label_Possession,Label_LosePossession.
GetModVAEP_GBT.scala/GetModVAEP_LR.scala
These files are identical, just one uses the GBT predictions and the other uses the Logistic Regression predictions.
These files compute the scores described above for each action and aggregate the JointVAEP score for each pair players throughout the season, the data is ranked in descending order by modified VAEP score, but the final dataframe has the modified score and the original score, and is stored at hdfs:///user/sa5064/Project/Results/GBT/resultsGBT.snappy.parquet or hdfs:///user/sa5064/Project/Results/LR/resultsLR.snappy.parquet depending on the file. It also shows the top 20 modified VAEP scores for the entire season.

