"""
Football Data Pipeline - Database Integration
Speichert API-Daten strukturiert für Trend-Analyse
"""

import json
import sqlite3
try:
    import psycopg2 # type: ignore
except ImportError:
    psycopg2 = None
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional, Any
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FootballDatabase:
    def __init__(self, db_type='sqlite', connection_params=None):
        """
        Initialize database connection
        
        Args:
            db_type: 'sqlite' or 'postgresql'
            connection_params: Dict with connection details
        """
        self.db_type = db_type
        self.connection_params = connection_params or {}
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            if self.db_type == 'sqlite':
                db_path = self.connection_params.get('database', 'football_data.db')
                self.connection = sqlite3.connect(db_path)
                self.connection.row_factory = sqlite3.Row
                logger.info(f"Connected to SQLite database: {db_path}")
                
            elif self.db_type == 'postgresql':
                if not psycopg2:
                    raise ImportError("psycopg2 is required for PostgreSQL connections")
                self.connection = psycopg2.connect(**self.connection_params)
                logger.info("Connected to PostgreSQL database")
                
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute query and return results"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            
            if query.strip().upper().startswith('SELECT'):
                if self.db_type == 'sqlite':
                    return [dict(row) for row in cursor.fetchall()]
                else:
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
            else:
                self.connection.commit()
                return []
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            self.connection.rollback()
            raise
    
    def store_fixture_data(self, fixture_data: Dict, collection_type: str):
        """
        Store complete fixture data including odds trends
        """
        try:
            fixture_info = fixture_data['game_info']
            api_data = fixture_data['data']
            
            # 1. Store/Update Teams
            home_team_id = self._store_team(fixture_info, 'home')
            away_team_id = self._store_team(fixture_info, 'away')
            
            # 2. Store/Update League
            league_id = self._store_league(fixture_info)
            
            # 3. Store/Update Fixture
            fixture_id = self._store_fixture(fixture_info, league_id, home_team_id, away_team_id)
            
            # 4. Store Odds History
            if 'odds_early' in api_data or 'odds_team_news' in api_data or 'odds_final' in api_data:
                self._store_odds_history(fixture_id, api_data, collection_type)
            
            # 5. Store Team Statistics  
            if 'home_team_stats' in api_data:
                self._store_team_statistics(home_team_id, league_id, api_data['home_team_stats'])
            if 'away_team_stats' in api_data:
                self._store_team_statistics(away_team_id, league_id, api_data['away_team_stats'])
            
            # 6. Store H2H Data
            if 'head_to_head' in api_data:
                self._store_head_to_head(home_team_id, away_team_id, api_data['head_to_head'])
            
            # 7. Store Lineups (wenn verfügbar)
            if 'lineups' in api_data:
                self._store_lineups(fixture_id, api_data['lineups'])
            
            # 8. Detect and store team events (injuries, suspensions)
            self._detect_team_events(fixture_info, api_data)
            
            logger.info(f"✅ Stored {collection_type} data for fixture {fixture_id}")
            return fixture_id
            
        except Exception as e:
            logger.error(f"❌ Error storing fixture data: {e}")
            raise
    
    def _store_team(self, fixture_info: Dict, team_type: str) -> int:
        """Store or update team information"""
        team_id = fixture_info[f'{team_type}_team_id']
        team_name = fixture_info[f'{team_type}_team']
        country = fixture_info.get('country', 'Unknown')
        
        self.execute_query("""
            INSERT OR REPLACE INTO teams (id, name, country, updated_at) 
            VALUES (?, ?, ?, ?)
        """, (team_id, team_name, country, datetime.now()))
        
        return team_id
    
    def _store_league(self, fixture_info: Dict) -> int:
        """Store or update league information"""
        league_id = fixture_info['league_id']
        league_name = fixture_info['league']
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1
        
        existing = self.execute_query(
            "SELECT id FROM leagues WHERE id = ? AND season = ?",
            (league_id, season)
        )
        
        if not existing:
            self.execute_query(
                """INSERT INTO leagues (id, name, country, season) 
                   VALUES (?, ?, ?, ?)""",
                (league_id, league_name, fixture_info.get('country', 'Unknown'), season)
            )
        
        return league_id
    
    def _store_fixture(self, fixture_info: Dict, league_id: int, home_id: int, away_id: int) -> int:
        """Store or update fixture"""
        fixture_id = fixture_info['fixture_id']
        kickoff = datetime.fromisoformat(fixture_info['kickoff_utc'].replace('Z', '+00:00'))
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1
        
        existing = self.execute_query(
            "SELECT id FROM fixtures WHERE id = ?",
            (fixture_id,)
        )
        
        if not existing:
            self.execute_query(
                """INSERT INTO fixtures 
                   (id, league_id, season, home_team_id, away_team_id, kickoff_utc, venue_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (fixture_id, league_id, season, home_id, away_id, kickoff, 
                 fixture_info.get('venue', 'Unknown'))
            )
        
        return fixture_id
    
    def _store_odds_history(self, fixture_id: int, api_data: Dict, collection_type: str):
        """Store odds with historical tracking"""
        odds_keys = [k for k in api_data.keys() if k.startswith('odds_')]
        for odds_key in odds_keys:
            odds_data = api_data[odds_key]
            if not odds_data or 'bookmakers' not in odds_data:
                logger.warning(f"No odds or bookmakers for key {odds_key} in fixture {fixture_id}")
                continue
            logger.info(f"Odds-Team-Mapping: {odds_data.get('home_team')} vs {odds_data.get('away_team')} (Fixture {fixture_id})")
            for bookmaker_data in odds_data['bookmakers']:
                bookmaker = bookmaker_data['title']
                for market in bookmaker_data.get('markets', []):
                    market_type = market['key']  # 'h2h', 'spreads', 'totals'
                    # Extract odds based on market type
                    home_odds = draw_odds = away_odds = None
                    over_odds = under_odds = handicap = total_points = None
                    
                    if market_type == 'h2h':
                        outcomes = {o['name']: o['price'] for o in market['outcomes']}
                        home_odds = outcomes.get(odds_data.get('home_team'))
                        away_odds = outcomes.get(odds_data.get('away_team'))
                        draw_odds = outcomes.get('Draw')
                        logger.info(f"Storing odds for fixture {fixture_id}, bookmaker {bookmaker}, market {market_type}: home={home_odds}, draw={draw_odds}, away={away_odds}")
                        if home_odds is None or away_odds is None:
                            logger.warning(f"No odds found for teams: {odds_data.get('home_team')} vs {odds_data.get('away_team')} in fixture {fixture_id} (bookmaker: {bookmaker})")
                        
                    elif market_type == 'spreads':
                        for outcome in market['outcomes']:
                            if outcome['name'] == odds_data['home_team']:
                                home_odds = outcome['price']
                                handicap = outcome.get('point')
                            elif outcome['name'] == odds_data['away_team']:
                                away_odds = outcome['price']
                                
                    elif market_type == 'totals':
                        for outcome in market['outcomes']:
                            if outcome['name'] == 'Over':
                                over_odds = outcome['price']
                                total_points = outcome.get('point')
                            elif outcome['name'] == 'Under':
                                under_odds = outcome['price']
                    
                    # Store in database
                    self.execute_query(
                        """INSERT INTO odds_history 
                           (fixture_id, bookmaker, market_type, home_odds, draw_odds, away_odds,
                            over_odds, under_odds, handicap, total_points, collected_at, collection_phase)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (fixture_id, bookmaker, market_type, home_odds, draw_odds, away_odds,
                         over_odds, under_odds, handicap, total_points, datetime.now(), collection_type)
                    )
    
    def _store_team_statistics(self, team_id: int, league_id: int, stats_data: Dict):
        """Store team statistics with time series"""
        if not stats_data or 'response' not in stats_data:
            return
            
        stats = stats_data['response']
        season = datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1
        collection_date = datetime.now().date()
        
        # Extract relevant statistics
        fixtures = stats.get('fixtures', {})
        goals = stats.get('goals', {})
        
        self.execute_query(
            """INSERT OR REPLACE INTO team_statistics
               (team_id, league_id, season, collection_date, matches_played, wins, draws, losses,
                goals_for, goals_against, win_percentage)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (team_id, league_id, season, collection_date,
             fixtures.get('played', {}).get('total', 0),
             fixtures.get('wins', {}).get('total', 0), 
             fixtures.get('draws', {}).get('total', 0),
             fixtures.get('loses', {}).get('total', 0),
             goals.get('for', {}).get('total', {}).get('total', 0),
             goals.get('against', {}).get('total', {}).get('total', 0),
             round((fixtures.get('wins', {}).get('total', 0) / max(fixtures.get('played', {}).get('total', 1), 1)) * 100, 2))
        )
    
    def _store_head_to_head(self, home_team_id: int, away_team_id: int, h2h_data: Dict):
        """Store head-to-head history data"""
        if not h2h_data or 'response' not in h2h_data:
            return
            
        for fixture in h2h_data['response']:
            fixture_id = fixture['fixture']['id']
            match_date = datetime.fromisoformat(fixture['fixture']['date'][:10])
            
            # Get score if available
            home_score = fixture.get('goals', {}).get('home')
            away_score = fixture.get('goals', {}).get('away')
            
            # Determine which team was home/away in this historical match
            historical_home_id = fixture['teams']['home']['id'] 
            historical_away_id = fixture['teams']['away']['id']
            
            self.execute_query(
                """INSERT OR IGNORE INTO head_to_head 
                   (home_team_id, away_team_id, fixture_id, home_score, away_score, 
                    match_date, league_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (historical_home_id, historical_away_id, fixture_id, 
                 home_score, away_score, match_date, fixture.get('league', {}).get('id'))
            )
    
    def _store_lineups(self, fixture_id: int, lineup_data: Dict):
        """Store lineup information"""
        if not lineup_data or 'response' not in lineup_data:
            return
            
        for lineup in lineup_data['response']:
            team_id = lineup['team']['id']
            formation = lineup.get('formation', 'Unknown')
            
            # Store starting XI
            for player in lineup.get('startXI', []):
                player_id = player['player']['id']
                player_name = player['player']['name']
                position = player.get('player', {}).get('pos', 'Unknown')
                
                # Store player if not exists
                self.execute_query(
                    """INSERT OR IGNORE INTO players (id, name, team_id, position)
                       VALUES (?, ?, ?, ?)""",
                    (player_id, player_name, team_id, position)
                )
                
                # Store lineup entry
                self.execute_query(
                    """INSERT OR REPLACE INTO lineups 
                       (fixture_id, team_id, formation, player_id, position, is_starter, is_captain)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (fixture_id, team_id, formation, player_id, position, 1, 
                     player.get('player', {}).get('captain', False))
                )
            
            # Store substitutes
            for player in lineup.get('substitutes', []):
                player_id = player['player']['id']
                player_name = player['player']['name']
                position = player.get('player', {}).get('pos', 'Unknown')
                
                # Store player if not exists
                self.execute_query(
                    """INSERT OR IGNORE INTO players (id, name, team_id, position)
                       VALUES (?, ?, ?, ?)""",
                    (player_id, player_name, team_id, position)
                )
                
                # Store lineup entry
                self.execute_query(
                    """INSERT OR REPLACE INTO lineups 
                       (fixture_id, team_id, formation, player_id, position, is_starter)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (fixture_id, team_id, formation, player_id, position, 0)
                )
    
    def _detect_team_events(self, fixture_info: Dict, api_data: Dict):
        """Detect injuries, suspensions from data changes"""
        # Simple event detection based on data changes
        # This could be enhanced with more sophisticated logic
        
        # Check for lineup changes that might indicate injuries/suspensions
        if 'lineups' in api_data:
            lineups = api_data['lineups']
            if lineups.get('response'):
                for lineup in lineups['response']:
                    team_id = lineup['team']['id']
                    formation = lineup.get('formation', 'Unknown')
                    
                    # Check for missing key players (basic implementation)
                    if lineup.get('startXI'):
                        # Could implement logic to detect when expected players are missing
                        pass
        
        # Could add more event detection logic here:
        # - Analysis of team news
        # - Comparison with previous lineups
        # - Integration with external injury databases
        pass
    
    def get_odds_trends(self, fixture_id: int) -> List[Dict]:
        """Get odds movement for a specific fixture"""
        return self.execute_query(
            """SELECT market_type, collection_phase, home_odds, draw_odds, away_odds, 
                      collected_at, bookmaker
               FROM odds_history 
               WHERE fixture_id = ? 
               ORDER BY market_type, collected_at""",
            (fixture_id,)
        )
    
    def get_team_form_analysis(self, team_id: int, days: int = 30) -> Dict:
        """Analyze team performance over time period"""
        cutoff_date = datetime.now().date() - timedelta(days=days)
        
        stats = self.execute_query(
            """SELECT * FROM team_statistics 
               WHERE team_id = ? AND collection_date >= ?
               ORDER BY collection_date DESC LIMIT 1""",
            (team_id, cutoff_date)
        )
        
        recent_fixtures = self.execute_query(
            """SELECT f.*, ht.name as home_team, at.name as away_team
               FROM fixtures f
               JOIN teams ht ON f.home_team_id = ht.id
               JOIN teams at ON f.away_team_id = at.id  
               WHERE (f.home_team_id = ? OR f.away_team_id = ?) 
               AND f.kickoff_utc >= ?
               ORDER BY f.kickoff_utc DESC""",
            (team_id, team_id, cutoff_date)
        )
        
        return {
            'current_stats': stats[0] if stats else None,
            'recent_fixtures': recent_fixtures,
            'analysis_period_days': days
        }
    
    def get_event_impact_analysis(self, team_id: int) -> Dict:
        """Analyze how team events correlate with odds movements"""
        events = self.execute_query(
            """SELECT * FROM team_events 
               WHERE team_id = ? AND start_date >= date('now', '-30 days')
               ORDER BY start_date DESC""",
            (team_id,)
        )
        
        # Find fixtures around event dates
        impact_analysis = []
        for event in events:
            nearby_odds = self.execute_query(
                """SELECT oh.*, f.kickoff_utc
                   FROM odds_history oh
                   JOIN fixtures f ON oh.fixture_id = f.id  
                   WHERE (f.home_team_id = ? OR f.away_team_id = ?)
                   AND date(f.kickoff_utc) BETWEEN date(?) AND date(?, '+7 days')""",
                (team_id, team_id, event['start_date'], event['start_date'])
            )
            
            impact_analysis.append({
                'event': event,
                'odds_movements': nearby_odds
            })
        
        return {
            'team_events': events,
            'impact_analysis': impact_analysis
        }
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

# Integration in GitHub Actions Workflow
def integrate_with_workflow():
    """
    Add this to your existing collect_game_data function:
    """
    example_integration = '''
    # In collect_game_data function, nach dem Speichern der JSON:
    
    # Initialize database 
    db_config = {
        'database': 'data/football_data.db'  # SQLite für GitHub Actions
        # Für PostgreSQL (in production):
        # 'host': os.getenv('DB_HOST'),
        # 'database': os.getenv('DB_NAME'), 
        # 'user': os.getenv('DB_USER'),
        # 'password': os.getenv('DB_PASSWORD')
    }
    
    db = FootballDatabase('sqlite', db_config)
    
    try:
        # Store in database
        fixture_id = db.store_fixture_data(collected_data, collection_type)
        print(f"✅ Data stored in database for fixture {fixture_id}")
        
        # Optional: Generate insights
        if collection_type == 'final_data':
            trends = db.get_odds_trends(fixture_id)
            print(f"📊 Found {len(trends)} odds data points")
            
    except Exception as e:
        print(f"❌ Database error: {e}")
    finally:
        db.close()
    '''
    
    return example_integration

if __name__ == "__main__":
    # Test database setup
    db = FootballDatabase('sqlite', {'database': 'test_football.db'})
    
    # Create tables (run schema first)
    print("Database integration ready!")
    print("Next steps:")
    print("1. Run database_schema.sql to create tables")  
    print("2. Add database integration to workflow")
    print("3. Set up dashboard connection")
    
    db.close()