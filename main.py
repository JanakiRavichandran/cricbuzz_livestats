import requests
import mysql.connector
import streamlit as st
import base64
import os
import sqlite3
import pandas as pd
import json
import logging

# ------------------ Cricbuzz API (Live Match Data into MySQL) ------------------ #
API_KEY = "4094bbee7dmsh42a522f6e656373p1d53b5jsn61b27c4c7b4a"
API_HOST = "cricbuzz-cricket.p.rapidapi.com"
HEADERS = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": API_HOST
}
BASE_URL = f"https://{API_HOST}"

# Database Config (MySQL for live matches)
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'sqlpassword',
    'database': 'cricket_db'
}

# ------------------ Fetch & Insert Live Matches ------------------ #
def fetch_matches(endpoint):
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching {endpoint}: {e}")
        return None

def insert_matches(data):
    if not data or "typeMatches" not in data:
        return

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    for type_match in data.get("typeMatches", []):
        for series_wrapper in type_match.get("seriesMatches", []):
            series = series_wrapper.get("seriesAdWrapper")
            if not series:
                continue

            for match in series.get("matches", []):
                info = match.get("matchInfo", {})
                cursor.execute("""
                    INSERT INTO cricket_matches (
                        match_id, series_id, series_name, match_desc, match_format,
                        team1_name, team2_name, status, start_date, end_date, state
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        status=VALUES(status), start_date=VALUES(start_date), end_date=VALUES(end_date)
                """, (
                    info.get('matchId'),
                    info.get('seriesId'),
                    series.get('seriesName'),
                    info.get('matchDesc'),
                    info.get('matchFormat'),
                    info.get('team1', {}).get('teamName'),
                    info.get('team2', {}).get('teamName'),
                    info.get('status'),
                    info.get('startDate'),
                    info.get('endDate'),
                    info.get('state')
                ))
    conn.commit()
    cursor.close()
    conn.close()
    print("Live matches loaded into MySQL successfully!")

# ------------------ Background Function ------------------ #
def set_bg_with_overlay(image_path="background.jpg"):
    if not os.path.isfile(image_path):
        st.error(f"Background image not found: {image_path}")
        return

    with open(image_path, "rb") as f:
        data = f.read()
    b64 = base64.b64encode(data).decode()

    st.markdown(
        f"""
        <style>
        .stApp {{
            background: linear-gradient(rgba(0,0,0,0.9), rgba(0,0,0,0.9)),
                        url("data:image/jpg;base64,{b64}");
            background-size: cover;
            background-position: center;
            color: white;
        }}
        </style>

        <div class="main">
            <h3>Creator: Janaki R</h3>
            <h5>Email: janakiravichandran2806@gmail.com</h5>
            <h5>LinkedIn: <a href='https://www.linkedin.com/in/janaki-ravichandran/' target='_blank'>Profile</a></h5>
        </div>
        """,
        unsafe_allow_html=True
    )

# Apply background globally
set_bg_with_overlay("background.jpg")

# ------------------ SQL Analysis Data (Cricsheet JSON -> SQLite) ------------------ #
SQLANALYSIS_DB = "sqlanalysis_db.sqlite"
DATA_DIR = "data/matches"   # path to your unzipped Cricsheet JSON files

def create_sqlanalysis_tables(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS matches")
    cur.execute("""
        CREATE TABLE matches (
            match_id TEXT PRIMARY KEY,
            season TEXT,
            date TEXT,
            venue TEXT,
            city TEXT,
            team1 TEXT,
            team2 TEXT,
            toss_winner TEXT,
            toss_decision TEXT,
            winner TEXT,
            result TEXT
        )
    """)
    cur.execute("DROP TABLE IF EXISTS deliveries")
    cur.execute("""
        CREATE TABLE deliveries (
            delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT,
            inning INTEGER,
            over INTEGER,
            ball INTEGER,
            batting_team TEXT,
            bowling_team TEXT,
            striker TEXT,
            non_striker TEXT,
            bowler TEXT,
            runs_batsman INTEGER,
            runs_extras INTEGER,
            runs_total INTEGER,
            wicket_kind TEXT,
            wicket_player_out TEXT,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    """)
    conn.commit()
    print("SQLAnalysis tables created successfully!")

def parse_match_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    match_id = os.path.basename(filepath).replace(".json", "")
    info = data.get("info", {})
    match_record = {
        "match_id": match_id,
        "season": str(info.get("season")),
        "date": str(info.get("dates", [""])[0]),
        "venue": info.get("venue"),
        "city": info.get("city"),
        "team1": (info.get("teams") or ["",""])[0],
        "team2": (info.get("teams") or ["",""])[1] if len(info.get("teams", []))>1 else "",
        "toss_winner": info.get("toss", {}).get("winner"),
        "toss_decision": info.get("toss", {}).get("decision"),
        "winner": info.get("outcome", {}).get("winner"),
        "result": json.dumps(info.get("outcome", {}))
    }
    deliveries = []
    for inning_index, inning in enumerate(data.get("innings", []), start=1):
        team = inning.get("team")
        for over in inning.get("overs", []):
            over_num = over.get("over")
            for delivery in over.get("deliveries", []):
                d = {
                    "match_id": match_id,
                    "inning": inning_index,
                    "over": over_num,
                    "ball": delivery.get("ball"),
                    "batting_team": team,
                    "bowling_team": delivery.get("bowler_team") or "",
                    "striker": delivery.get("batter"),
                    "non_striker": delivery.get("non_striker"),
                    "bowler": delivery.get("bowler"),
                    "runs_batsman": delivery.get("runs", {}).get("batter",0),
                    "runs_extras": delivery.get("runs", {}).get("extras",0),
                    "runs_total": delivery.get("runs", {}).get("total",0),
                    "wicket_kind": (delivery.get("wickets",[{}])[0].get("kind") if delivery.get("wickets") else None),
                    "wicket_player_out": (delivery.get("wickets",[{}])[0].get("player_out") if delivery.get("wickets") else None)
                }
                deliveries.append(d)
    return match_record, deliveries

def load_sqlanalysis_data():
    conn = sqlite3.connect(SQLANALYSIS_DB)
    create_sqlanalysis_tables(conn)
    cur = conn.cursor()
    matches_inserted = 0
    deliveries_inserted = 0

    for file in os.listdir(DATA_DIR):
        if not file.endswith(".json"):
            continue
        filepath = os.path.join(DATA_DIR, file)
        try:
            match_record, deliveries = parse_match_file(filepath)
            # Insert match
            cur.execute("""
                INSERT OR REPLACE INTO matches
                (match_id, season, date, venue, city, team1, team2, toss_winner, toss_decision, winner, result)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, list(match_record.values()))
            matches_inserted +=1
            # Insert deliveries
            for d in deliveries:
                cur.execute("""
                    INSERT INTO deliveries
                    (match_id, inning, over, ball, batting_team, bowling_team, striker, non_striker, bowler,
                     runs_batsman, runs_extras, runs_total, wicket_kind, wicket_player_out)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, list(d.values()))
            deliveries_inserted += len(deliveries)
        except Exception as e:
            print(f"Skipping {file} due to error: {e}")

    conn.commit()
    print(f"Inserted {matches_inserted} matches and {deliveries_inserted} deliveries into SQLAnalysis DB.")
    conn.close()

# ------------------ Main Execution ------------------ #
if __name__ == "__main__":
    # Load live matches to MySQL
    for endpoint in ["matches/v1/live", "matches/v1/recent", "matches/v1/upcoming"]:
        data = fetch_matches(endpoint)
        insert_matches(data)

    # Load Cricsheet JSON into SQLite for SQL analysis
    load_sqlanalysis_data()
