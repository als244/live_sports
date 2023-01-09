import pandas as pd
import numpy as np
import pickle
import datetime
import time
import json
import glob

# CAN RUN THIS SCRIPT TO AGGREGATE ALL OF THE COLLECTED ODDS RESPONSES (FROM GIVEN DAY) INTO A DATAFRAME

ts = time.time()
time_human = datetime.datetime.fromtimestamp(ts).strftime('%H_%M_%S')
dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

book_odds = []

today = datetime.date.today()

DATE_STR = today.strftime("%Y%m%d")
ODDS_PREF = "/mnt/storage/data/live_sports/nba/odds/" + DATE_STR

my_respose_files = glob.glob(ODDS_PREF + "/responses/*")

for f in my_respose_files:
	odds_response = pickle.load(open(f, "rb"))
	json_response = json.loads(odds_response.text)
	file_split = f.split("/")
	query_time = file_split[-1].split(".")[0]
	for g in json_response:
		game_id = g['id']
		commence_time = g["commence_time"]
		home_team = g["home_team"]
		away_team = g["away_team"]
		bookmakers = g["bookmakers"]
		for book in bookmakers:
			book_name = book["key"]
			book_last_update = book["last_update"]
			markets = book["markets"]
			home_odds, away_odds, home_spread, home_spread_odds, away_spread, away_spread_odds = np.nan, np.nan, np.nan, np.nan, np.nan, np.nan
			over_points, over_odds, under_points, under_odds = np.nan, np.nan, np.nan, np.nan
			for m in markets:
				if m["key"] == "h2h":
					for d in m["outcomes"]:
						if d["name"] == home_team:
							home_odds = d["price"]
						if d["name"] == away_team:
							away_odds = d["price"]
				if m["key"] == "spreads":
					for d in m["outcomes"]:
						if d["name"] == home_team:
							home_spread = d["point"]
							home_spread_odds = d["price"]
						if d["name"] == away_team:
							away_spread = d["point"]
							away_spread_odds = d["price"]
				if m["key"] == "totals":
					for d in m["outcomes"]:
						if d["name"] == "Over":
							over_points = d["point"]
							over_odds = d["price"]
						if d["name"] == "Under":
							under_points = d["point"]
							under_odds = d["price"]
			row = [query_time, game_id, commence_time, home_team, away_team, book_name, book_last_update, home_odds, away_odds, home_spread, home_spread_odds, away_spread, away_spread_odds, over_points, over_odds, under_points, under_odds]
			book_odds.append(row)


odds_df = pd.DataFrame(book_odds, columns = ["query_time", "game_id", "commence_time", "home_team", "away_team", "book_name", "book_last_update", "home_odds", "away_odds", "home_spread", "home_spread_odds", "away_spread", "away_spread_odds", "over_points", "over_odds", "under_points", "under_odds"])

## annotate the raw data

# remove usless rows
odds_df = odds_df.loc[~((odds_df["home_odds"] == 1) & (odds_df["away_odds"] == 1))]

# convert odds to probabilities

# moneyline
odds_df["home_prob"] = round((1 / odds_df["home_odds"]) * 100, 2)
odds_df["away_prob"] = round((1 / odds_df["away_odds"]) * 100, 2)
odds_df["book_edge_prob"] = round(odds_df["home_prob"] + odds_df["away_prob"] - 100, 2)

# spreads
odds_df["home_spread_prob"] = round((1 / odds_df["home_spread_odds"]) * 100, 2)
odds_df["away_spread_prob"] = round((1 / odds_df["away_spread_odds"]) * 100, 2)

# totals
odds_df["over_odds"] = round((1 / odds_df["over_odds"]) * 100, 2)
odds_df["under_odds"] = round((1 / odds_df["under_odds"]) * 100, 2)

odds_df_sorted = odds_df.sort_values(by=["query_time", "game_id", "book_name", "book_last_update"])

pickle.dump(odds_df_sorted, open(ODDS_PREF + "/" + time_human + ".pickle", "wb"))