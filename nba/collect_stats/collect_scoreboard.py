import requests
import json
import pickle
import datetime
import time
import pandas as pd
from datetime import date


#### RUN THIS SCRIPT ONCE DAILY

scoreboard_resp = requests.get(
        f'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json'
    )

scoreboard_json = scoreboard_resp.json()

scoreboard = scoreboard_json["scoreboard"]

scoreboard_date = scoreboard["gameDate"]

today = date.today()

SCOREBOARD_PREF = "/mnt/storage/data/live_sports/nba/stats/scoreboards"
DATE_STR = today.strftime("%Y%m%d")

games = scoreboard["games"]

my_scoreboard = []

for g in games:
	game_data = [g["gameId"], g["gameCode"], g["gameTimeUTC"], g["homeTeam"]["teamCity"] + " " + g["homeTeam"]["teamName"], g["homeTeam"]["teamId"], g["homeTeam"]["teamTricode"], str(g["homeTeam"]["wins"]) + "-" + str(g["homeTeam"]["losses"]), 
			g["awayTeam"]["teamCity"] + " " + g["awayTeam"]["teamName"], g["awayTeam"]["teamId"], g["awayTeam"]["teamTricode"], str(g["awayTeam"]["wins"]) + "-" + str(g["awayTeam"]["losses"])]
	my_scoreboard.append(game_data)

scoreboard_df = pd.DataFrame(my_scoreboard, columns=["game_id", "game_code", "game_time_UTC", "home_team", "home_team_id", "home_team_tricode", "home_record", "away_team", "away_team_id", "away_team_tricode", "away_record"])

pickle.dump(scoreboard_df, open(SCOREBOARD_PREF + "/" + DATE_STR + ".pickle", "wb"))