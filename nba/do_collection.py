import requests
import json
import pickle
import datetime
import time
import pytz
from pytz import timezone
import pandas as pd
import numpy as np
import os
from datetime import date, timedelta
from threading import Thread


# call this once a day
def collect_scoreboard():
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

	print("COLLECTING SCOREBOARD ON: " + DATE_STR)

	my_scoreboard = []

	for g in games:
		game_data = [g["gameId"], g["gameCode"], g["gameTimeUTC"], g["homeTeam"]["teamCity"] + " " + g["homeTeam"]["teamName"], g["homeTeam"]["teamId"], g["homeTeam"]["teamTricode"], str(g["homeTeam"]["wins"]) + "-" + str(g["homeTeam"]["losses"]), 
				g["awayTeam"]["teamCity"] + " " + g["awayTeam"]["teamName"], g["awayTeam"]["teamId"], g["awayTeam"]["teamTricode"], str(g["awayTeam"]["wins"]) + "-" + str(g["awayTeam"]["losses"])]
		my_scoreboard.append(game_data)

	scoreboard_df = pd.DataFrame(my_scoreboard, columns=["game_id", "game_code", "game_time_UTC", "home_team", "home_team_id", "home_team_tricode", "home_record", "away_team", "away_team_id", "away_team_tricode", "away_record"])

	## getting times to bound collection process
	scoreboard_df["game_time_UTC"] = pd.to_datetime(scoreboard_df["game_time_UTC"])
	scoreboard_df["game_time_mountain"] = scoreboard_df["game_time_UTC"].dt.tz_convert('America/Denver')
	start, end = min(scoreboard_df["game_time_mountain"]), max(scoreboard_df["game_time_mountain"])
	game_bounds = [start, end, len(my_scoreboard)]
	pickle.dump(game_bounds, open(SCOREBOARD_PREF + "/" + DATE_STR + "_game_bounds.pickle", "wb"))

	## dumping scoreboard
	pickle.dump(scoreboard_df, open(SCOREBOARD_PREF + "/" + DATE_STR + ".pickle", "wb"))

	return game_bounds


## spawn new thread to launch this every day
def collect_playbyplay(start, end):

	today = date.today()

	SCOREBOARD_PREF = "/mnt/storage/data/live_sports/nba/stats/scoreboards"
	DATE_STR = today.strftime("%Y%m%d")

	scoreboard_df = pickle.load(open(SCOREBOARD_PREF + "/" + DATE_STR + ".pickle", "rb"))

	game_ids = scoreboard_df["game_id"]

	PLAYBYPLAY_PREF = "/mnt/storage/data/live_sports/nba/stats/playbyplay"

	if not os.path.exists(PLAYBYPLAY_PREF + "/" + DATE_STR):
		os.makedirs(PLAYBYPLAY_PREF + "/" + DATE_STR)

	LOOP_FREQ = 15.0

	START_TIME = time.time()

	print("COLLECTING PLAY BY PLAY ON: " + DATE_STR)

	### KEEP SCRIPT RUNNING DURING GAMES TO COLLECT UPDATED PLAY BY PLAY

	### WOULD LIKE TO MAKE THIS STREAMING INSTEAD OF BUILDING UP NEW FRAMES AND REPEATING DATA...
	while True:
		ts = time.time()
		time_human = datetime.datetime.fromtimestamp(ts).strftime('%H_%M_%S')
		dt_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		dt = datetime.datetime.fromtimestamp(ts).astimezone(timezone('America/Denver'))

		## only collect between start and end bounds
		if dt < start:
			time.sleep(60)
			continue

		if dt > end:
			return

		action_attributes = ["actionNumber", "clock", "timeActual", "period", "scoreHome", "scoreAway", "teamId", "teamTricode", "actionType", "subType", "descriptor", "description", "personId", "playerName", "isFieldGoal", "shotResult", "x", "y", "area", "areaDetail", "side", "shotDistance", "shotActionNumber", "possession", "edited"]
	
		for g in game_ids:
		
			if not os.path.exists(PLAYBYPLAY_PREF + "/" + DATE_STR + "/" + str(g)):
				os.makedirs(PLAYBYPLAY_PREF + "/" + DATE_STR + "/" + str(g))

			playbyplay_req_str = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_" + str(g) + ".json"
			playbyplay_resp = requests.get(playbyplay_req_str)

			# happens for games that haven't started
			if playbyplay_resp.status_code != 200:
				print(f'Failed to get play by play. Game: {g}, Time: {dt}')
				continue

			p_json = playbyplay_resp.json()

			plays = p_json["game"]["actions"]

			playbyplay_rows = []

			for p in plays:
				action = []
				for a in action_attributes:
					if a in p:
						action.append(p[a])
					else:
						action.append(np.nan)
				playbyplay_rows.append(action)

				playbyplay_df = pd.DataFrame(playbyplay_rows, columns=action_attributes)
			pickle.dump(playbyplay_df, open(PLAYBYPLAY_PREF + "/" + DATE_STR + "/" + str(g) + "/" + time_human + ".pickle", "wb"))
	
		time.sleep(LOOP_FREQ - ((time.time() - START_TIME) % LOOP_FREQ))


## spawn new thread to launch this every day
def collect_boxscore(start, end):

	today = date.today()

	SCOREBOARD_PREF = "/mnt/storage/data/live_sports/nba/stats/scoreboards"
	DATE_STR = today.strftime("%Y%m%d")

	scoreboard_df = pickle.load(open(SCOREBOARD_PREF + "/" + DATE_STR + ".pickle", "rb"))

	game_ids = scoreboard_df["game_id"]

	BOXSCORE_PREF = "/mnt/storage/data/live_sports/nba/stats/boxscores/"

	if not os.path.exists(BOXSCORE_PREF + DATE_STR):
		os.makedirs(BOXSCORE_PREF + DATE_STR)

	LOOP_FREQ = 15.0

	print("COLLECTING BOXSCORES ON: " + DATE_STR)

	START_TIME = time.time()

	### KEEP SCRIPT RUNNING DURING GAMES TO COLLECT UPDATED BOXSCORES

	while True:
		ts = time.time()
		time_human = datetime.datetime.fromtimestamp(ts).strftime('%H_%M_%S')
		dt_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		dt = datetime.datetime.fromtimestamp(ts).astimezone(timezone('America/Denver'))
		
		## only collect between start and end bounds
		if dt < start:
			time.sleep(60)
			continue

		if dt > end:
			return

		boxscore_rows = []

		stats = ["assists", "benchPoints", "blocks", "fieldGoalsAttempted", "fieldGoalsMade", "foulsOffensive", "foulsDrawn", "foulsPersonal", "foulsTeam", "freeThrowsAttempted", "freeThrowsMade", 
				"pointsFastBreak", "pointsFromTurnovers", "pointsInThePaint", "reboundsDefensive", "reboundsOffensive", "reboundsTotal", "secondChancePointsAttempted", "secondChancePointsMade", "steals", 
				"threePointersAttempted", "threePointersMade", "turnovers", "twoPointersAttempted", "twoPointersMade"]
	
		for g in game_ids:

			req_str = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_" + str(g) + ".json"
			boxscore_resp = requests.get(req_str)

			# happens for games that haven't started
			if boxscore_resp.status_code != 200:
				print(f'Failed to get boxscore. Game: {g}, Time: {dt}')
				continue

			b_json = boxscore_resp.json()

			b = b_json["game"]

			game_boxscore = [b["gameId"], b["gameStatusText"], b["gameClock"], b["homeTeam"]["teamId"], b["homeTeam"]["teamTricode"], b["homeTeam"]["score"], b["awayTeam"]["teamId"], b["awayTeam"]["teamTricode"], b["awayTeam"]["score"]]
			for t in ["homeTeam", "awayTeam"]:
				for s in stats:
					game_boxscore.append(b[t]["statistics"][s])

		
			boxscore_rows.append(game_boxscore)

		my_columns = ["gameId", "gameStatusText", "gameClock", "home_teamId", "home_teamTricode", "home_score", "away_teamId", "away_teamTricode", "away_score"]
		for t in ["home", "away"]:
			for s in stats:
				my_columns.append(t + "_" + s)

		boxscore_df = pd.DataFrame(boxscore_rows, columns=my_columns)
		pickle.dump(boxscore_df, open(BOXSCORE_PREF + DATE_STR + "/" + time_human + ".pickle", "wb"))

		time.sleep(LOOP_FREQ - ((time.time() - START_TIME) % LOOP_FREQ))


def collect_odds_responses(start, end):

	# An api key is emailed to you when you sign up to a plan
	# Get a free API key at https://api.the-odds-api.com/
	API_KEY = '5f7427a67cf56b8e46c42f6aa3671da0'
	SPORT = "basketball_nba"
	REGIONS = 'us' # uk | us | eu | au. Multiple can be specified if comma delimited

	MARKETS = 'h2h,spreads,totals' # h2h | spreads | totals. Multiple can be specified if comma delimited

	ODDS_FORMAT = 'decimal' # decimal | american

	DATE_FORMAT = 'unix' # iso | unix

	LOOP_FREQ = 15.0

	today = date.today()

	DATE_STR = today.strftime("%Y%m%d")
	ODDS_PREF = "/mnt/storage/data/live_sports/nba/odds/" + DATE_STR

	print("COLLECTING ODDS RESPONSES ON: " + DATE_STR)

	if not os.path.exists(ODDS_PREF):
		os.makedirs(ODDS_PREF)

	if not os.path.exists(ODDS_PREF + "/responses"):
		os.makedirs(ODDS_PREF + "/responses")

	start_time = time.time()


	### CAN LEAVE THIS RUNNING TO KEEP GENERATING UPDATED LIVE ODDS

	## BE CAREFUL OF API LIMITS...

	while True:
		ts = time.time()
		time_human = datetime.datetime.fromtimestamp(ts).strftime('%H_%M_%S')
		dt_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		dt = datetime.datetime.fromtimestamp(ts).astimezone(timezone('America/Denver'))

		## only collect between start and end bounds
		if dt < start:
			time.sleep(60)
			continue

		## after finished, build the DF from responses
		if dt > end:
			build_odds_df_from_responses(DATE_STR)
			return
		

		odds_response = requests.get(
			f'https://api.the-odds-api.com/v4/sports/{SPORT}/odds/',
			params={
				'api_key': API_KEY,
				'regions': REGIONS,
				'markets': MARKETS,
				'oddsFormat': ODDS_FORMAT,
				'dateFormat': DATE_FORMAT,
			}
		)

		if odds_response.status_code != 200:
			print(f'Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}')

		else:
			odds_json = odds_response.json()
			print('Number of events:', len(odds_json))
			print(odds_json)

			# Check the usage quota
			print('Remaining requests', odds_response.headers['x-requests-remaining'])
			print('Used requests', odds_response.headers['x-requests-used'])


			pickle.dump(odds_response, open(ODDS_PREF + "/" + "responses" + "/" + time_human + ".pickle", "wb"))
			time.sleep(LOOP_FREQ - ((time.time() - start_time) % LOOP_FREQ))


def build_odds_df_from_responses(date_str):

	print("BUILDING ODDS RESPONSE for: " + DATE_STR)

	ts = time.time()
	time_human = datetime.datetime.fromtimestamp(ts).strftime('%H_%M_%S')
	dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

	ODDS_PREF = "/mnt/storage/data/live_sports/nba/odds/" + date_str

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

	return


def main():


	today = date.today()
	date_str = today.strftime("%Y%m%d")
	prev_date_str = None

	while True:

		ts = time.time()
		time_human = datetime.datetime.fromtimestamp(ts).strftime('%H_%M_%S')
		dt_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		dt = datetime.datetime.fromtimestamp(ts)

		# run everyday around 8 am
		if (prev_date_str == date_str) or (dt.hour < 8):
			time.sleep(3600)
			continue

		game_bounds = collect_scoreboard()

		collect_start = game_bounds[0] - timedelta(hours=0, minutes=15)
		collect_end = game_bounds[1] + timedelta(hours=3, minutes=30)
		n_games = game_bounds[2]

		if n_games > 0:

			print("\n**** RUNNING THREADS FOR DAY: " + date_str + " ****")

			boxscore_thread = Thread(target=collect_boxscore, args=(collect_start, collect_end))
			play_by_play_thread = Thread(target=collect_playbyplay, args=(collect_start, collect_end))
			odds_response_thread = Thread(target=collect_odds_responses, args=(collect_start, collect_end))

			boxscore_thread.start()
			play_by_play_thread.start()
			odds_response_thread.start()

			boxscore_thread.join()
			play_by_play_thread.join()
			odds_response_thread.join()

			print("Threads finished for day: " + date_str + '\n\n')
			## wait 6 hours
			time.sleep(60 * 60 * 6)

		prev_date_str = date_str
		today = date.today()
		date_str = today.strftime("%Y%m%d")









if __name__ == '__main__':
	main()