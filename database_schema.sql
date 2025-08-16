-- Football Data Pipeline - Database Schema
-- SQLite Schema (für GitHub Actions) oder PostgreSQL (für Production)

-- Teams Table
CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT,
    logo_url TEXT,
    founded INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Leagues Table
CREATE TABLE IF NOT EXISTS leagues (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT,
    season INTEGER NOT NULL,
    logo_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(id, season)
);

-- Fixtures Table
CREATE TABLE IF NOT EXISTS fixtures (
    id INTEGER PRIMARY KEY,
    league_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    kickoff_utc TIMESTAMP NOT NULL,
    venue_name TEXT,
    status TEXT DEFAULT 'scheduled',
    home_score INTEGER,
    away_score INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (league_id) REFERENCES leagues(id),
    FOREIGN KEY (home_team_id) REFERENCES teams(id),
    FOREIGN KEY (away_team_id) REFERENCES teams(id)
);

-- Odds History Table (Kern der Pipeline)
CREATE TABLE IF NOT EXISTS odds_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id INTEGER NOT NULL,
    bookmaker TEXT NOT NULL,
    market_type TEXT NOT NULL, -- 'h2h', 'spreads', 'totals'
    home_odds REAL,
    draw_odds REAL,
    away_odds REAL,
    over_odds REAL,
    under_odds REAL,
    handicap REAL,
    total_points REAL,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    collection_phase TEXT NOT NULL, -- 'early_odds', 'team_news', 'final_data'
    FOREIGN KEY (fixture_id) REFERENCES fixtures(id),
    INDEX idx_fixture_odds (fixture_id, market_type, collected_at),
    INDEX idx_collection_phase (collection_phase, collected_at)
);

-- Team Statistics (für Performance-Analyse)
CREATE TABLE IF NOT EXISTS team_statistics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id INTEGER NOT NULL,
    league_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    collection_date DATE NOT NULL,
    matches_played INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    draws INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    goals_for INTEGER DEFAULT 0,
    goals_against INTEGER DEFAULT 0,
    win_percentage REAL DEFAULT 0,
    form_last_5 TEXT, -- 'WWDLL' etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (team_id) REFERENCES teams(id),
    FOREIGN KEY (league_id) REFERENCES leagues(id),
    UNIQUE(team_id, league_id, season, collection_date)
);

-- Team Events (Verletzungen, Sperren, Transfers)
CREATE TABLE IF NOT EXISTS team_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id INTEGER NOT NULL,
    player_id INTEGER,
    event_type TEXT NOT NULL, -- 'injury', 'suspension', 'transfer', 'lineup_change'
    event_description TEXT,
    severity TEXT, -- 'minor', 'major', 'season_ending'
    start_date DATE NOT NULL,
    end_date DATE,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT, -- 'api', 'manual', 'news_scraping'
    FOREIGN KEY (team_id) REFERENCES teams(id),
    INDEX idx_team_events (team_id, event_type, start_date)
);

-- Players Table (optional, für detaillierte Analyse)
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    team_id INTEGER,
    position TEXT,
    age INTEGER,
    nationality TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

-- Head-to-Head Records (für bessere Predictions)
CREATE TABLE IF NOT EXISTS head_to_head (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    fixture_id INTEGER NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    match_date DATE NOT NULL,
    league_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (home_team_id) REFERENCES teams(id),
    FOREIGN KEY (away_team_id) REFERENCES teams(id),
    FOREIGN KEY (fixture_id) REFERENCES fixtures(id),
    INDEX idx_h2h (home_team_id, away_team_id, match_date)
);

-- Lineups Table (wenn verfügbar vor Spielen)
CREATE TABLE IF NOT EXISTS lineups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    formation TEXT,
    player_id INTEGER,
    position TEXT,
    is_starter BOOLEAN DEFAULT 0,
    is_captain BOOLEAN DEFAULT 0,
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fixture_id) REFERENCES fixtures(id),
    FOREIGN KEY (team_id) REFERENCES teams(id),
    FOREIGN KEY (player_id) REFERENCES players(id)
);

-- Views für einfache Abfragen
CREATE VIEW IF NOT EXISTS upcoming_games_with_odds AS
SELECT 
    f.id as fixture_id,
    f.kickoff_utc,
    ht.name as home_team,
    at.name as away_team,
    l.name as league,
    l.country,
    oh.home_odds,
    oh.draw_odds,
    oh.away_odds,
    oh.bookmaker,
    oh.collected_at as odds_updated
FROM fixtures f
JOIN teams ht ON f.home_team_id = ht.id
JOIN teams at ON f.away_team_id = at.id  
JOIN leagues l ON f.league_id = l.id
LEFT JOIN (
    SELECT DISTINCT fixture_id, home_odds, draw_odds, away_odds, bookmaker, collected_at,
           ROW_NUMBER() OVER (PARTITION BY fixture_id ORDER BY collected_at DESC) as rn
    FROM odds_history 
    WHERE market_type = 'h2h'
) oh ON f.id = oh.fixture_id AND oh.rn = 1
WHERE f.kickoff_utc > datetime('now')
AND f.status = 'scheduled'
ORDER BY f.kickoff_utc;

-- Index für Performance
CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff ON fixtures(kickoff_utc);
CREATE INDEX IF NOT EXISTS idx_odds_collected ON odds_history(collected_at);
CREATE INDEX IF NOT EXISTS idx_team_stats_date ON team_statistics(collection_date);
CREATE INDEX IF NOT EXISTS idx_events_detected ON team_events(detected_at);