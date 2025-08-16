"""
Enhanced API Mapping & Team Name Resolution
Intelligentes Team-Name-Mapping zwischen API-Football und The Odds API

7 verschiedene Matching-Strategien:
1. Exact matching
2. Mapping table lookup  
3. Normalized string matching
4. Substring matching
5. Word-based matching
6. Fuzzy string matching
7. League context matching

Mit automatischem Lernen und Performance-Tracking
"""

import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import difflib
from dataclasses import dataclass, asdict
import re
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MappingResult:
    """Result of a team name mapping attempt"""
    api_football_name: str
    odds_api_name: str
    confidence: float
    strategy_used: str
    match_found: bool
    alternatives: List[str]
    processing_time: float

@dataclass
class MappingStats:
    """Statistics for mapping performance"""
    total_attempts: int
    successful_mappings: int
    failed_mappings: int
    success_rate: float
    avg_confidence: float
    strategy_usage: Dict[str, int]
    last_updated: str

class EnhancedTeamMapper:
    """
    Enhanced team name mapping with multiple strategies and learning capability
    """
    
    def __init__(self, db_path: str = 'data/football_data.db', learn_mappings: bool = True):
        self.db_path = db_path
        self.learn_mappings = learn_mappings
        self.manual_mappings = self._load_manual_mappings()
        self.learned_mappings = self._load_learned_mappings()
        self.normalization_rules = self._load_normalization_rules()
        self.stats = self._load_mapping_stats()
        
        # Initialize database for mapping storage
        self._init_mapping_database()
    
    def _init_mapping_database(self):
        """Initialize database tables for mapping storage"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS team_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_football_name TEXT NOT NULL,
                    odds_api_name TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    strategy_used TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    verified BOOLEAN DEFAULT 0,
                    league_context TEXT,
                    UNIQUE(api_football_name, odds_api_name, league_context)
                );
                
                CREATE TABLE IF NOT EXISTS mapping_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_football_name TEXT NOT NULL,
                    odds_api_name TEXT,
                    confidence REAL,
                    strategy_used TEXT,
                    success BOOLEAN NOT NULL,
                    processing_time REAL,
                    alternatives TEXT, -- JSON array
                    attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    league_context TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_mappings_api_name ON team_mappings(api_football_name);
                CREATE INDEX IF NOT EXISTS idx_mappings_odds_name ON team_mappings(odds_api_name);
                CREATE INDEX IF NOT EXISTS idx_attempts_date ON mapping_attempts(attempted_at);
            """)
            conn.commit()
            conn.close()
            logger.info("Mapping database initialized")
        except Exception as e:
            logger.error(f"Failed to initialize mapping database: {e}")
    
    def _load_manual_mappings(self) -> Dict[str, str]:
        """Load manually curated team name mappings"""
        manual_mappings = {
            # Premier League
            "Manchester United": "Manchester Utd",
            "Manchester City": "Manchester City",
            "Tottenham Hotspur": "Tottenham",
            "West Ham United": "West Ham",
            "Newcastle United": "Newcastle",
            "Aston Villa": "Aston Villa",
            "Brighton & Hove Albion": "Brighton",
            "Crystal Palace": "Crystal Palace",
            "Wolverhampton Wanderers": "Wolves",
            "Sheffield United": "Sheffield Utd",
            "Leicester City": "Leicester",
            "Nottingham Forest": "Nottm Forest",
            
            # La Liga
            "Real Madrid": "Real Madrid",
            "FC Barcelona": "Barcelona",
            "Atletico Madrid": "Atl Madrid",
            "Real Betis": "Real Betis",
            "Real Sociedad": "Real Sociedad",
            "Athletic Club": "Athletic Bilbao",
            "Villarreal CF": "Villarreal",
            "Valencia CF": "Valencia",
            "Sevilla FC": "Sevilla",
            "Real Mallorca": "Mallorca",
            "Deportivo Alaves": "Deportivo Alavés",
            "Cadiz CF": "Cádiz",
            "Celta Vigo": "Celta Vigo",
            
            # Bundesliga
            "Bayern Munich": "Bayern Munich",
            "Borussia Dortmund": "Dortmund",
            "RB Leipzig": "RB Leipzig",
            "Bayer Leverkusen": "Bayer Leverkusen",
            "Eintracht Frankfurt": "E. Frankfurt",
            "Borussia Monchengladbach": "B. Monchengladbach",
            "VfB Stuttgart": "Stuttgart",
            "SC Freiburg": "Freiburg",
            "TSG Hoffenheim": "Hoffenheim",
            "1. FC Koln": "FC Köln",
            "Hertha Berlin": "Hertha",
            "VfL Wolfsburg": "Wolfsburg",
            
            # Serie A
            "Juventus": "Juventus",
            "AC Milan": "AC Milan",
            "Inter": "Inter Milan",
            "AS Roma": "AS Roma",
            "SSC Napoli": "Napoli",
            "Lazio": "Lazio",
            "Atalanta": "Atalanta",
            "Fiorentina": "Fiorentina",
            "Torino": "Torino",
            "Bologna": "Bologna",
            "Udinese": "Udinese",
            "Sassuolo": "Sassuolo",
            
            # Ligue 1
            "Paris Saint Germain": "PSG",
            "Olympique Marseille": "Marseille",
            "Olympique Lyonnais": "Lyon",
            "AS Monaco": "Monaco",
            "Lille": "Lille",
            "Rennes": "Rennes",
            "OGC Nice": "Nice",
            "RC Strasbourg Alsace": "Strasbourg",
            "Montpellier": "Montpellier",
        }
        
        # Try to load additional mappings from file
        try:
            mapping_file = 'data/manual_team_mappings.json'
            if os.path.exists(mapping_file):
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    file_mappings = json.load(f)
                manual_mappings.update(file_mappings)
                logger.info(f"Loaded {len(file_mappings)} additional manual mappings")
        except Exception as e:
            logger.warning(f"Could not load manual mappings file: {e}")
        
        return manual_mappings
    
    def _load_learned_mappings(self) -> Dict[str, str]:
        """Load previously learned team name mappings from database"""
        learned = {}
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute("""
                SELECT api_football_name, odds_api_name 
                FROM team_mappings 
                WHERE verified = 1 OR confidence > 0.9
                ORDER BY confidence DESC
            """)
            
            for row in cursor.fetchall():
                learned[row[0]] = row[1]
                
            conn.close()
            logger.info(f"Loaded {len(learned)} learned mappings from database")
        except Exception as e:
            logger.warning(f"Could not load learned mappings: {e}")
        
        return learned
    
    def _load_normalization_rules(self) -> Dict[str, str]:
        """Load text normalization rules for team names"""
        return {
            # Common abbreviations and expansions
            r'\bFC\b': '',
            r'\bCF\b': '',
            r'\bAC\b': '',
            r'\bSC\b': '',
            r'\bASC\b': '',
            r'\bReal\b': 'Real',
            r'\bClub\b': '',
            r'\bAtletico\b': 'Atletico',
            r'\bBorussia\b': 'Borussia',
            r'\bOlympique\b': '',
            r'\bSporting\b': '',
            r'\bUnited\b': 'Utd',
            r'\bCity\b': 'City',
            r'\bHotspur\b': '',
            r'&': 'and',
            
            # Accented characters
            'é': 'e',
            'è': 'e',
            'ê': 'e',
            'ë': 'e',
            'á': 'a',
            'à': 'a',
            'â': 'a',
            'ã': 'a',
            'ä': 'a',
            'í': 'i',
            'ì': 'i',
            'î': 'i',
            'ï': 'i',
            'ó': 'o',
            'ò': 'o',
            'ô': 'o',
            'õ': 'o',
            'ö': 'o',
            'ú': 'u',
            'ù': 'u',
            'û': 'u',
            'ü': 'u',
            'ç': 'c',
            'ñ': 'n',
        }
    
    def _load_mapping_stats(self) -> MappingStats:
        """Load mapping statistics from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_attempts,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed,
                    AVG(CASE WHEN success = 1 THEN confidence END) as avg_confidence
                FROM mapping_attempts
            """)
            
            row = cursor.fetchone()
            if row and row[0] > 0:
                total, successful, failed, avg_conf = row
                success_rate = successful / total if total > 0 else 0.0
                
                # Get strategy usage
                cursor = conn.execute("""
                    SELECT strategy_used, COUNT(*) 
                    FROM mapping_attempts 
                    WHERE success = 1 
                    GROUP BY strategy_used
                """)
                strategy_usage = dict(cursor.fetchall())
                
                stats = MappingStats(
                    total_attempts=total,
                    successful_mappings=successful,
                    failed_mappings=failed,
                    success_rate=success_rate,
                    avg_confidence=avg_conf or 0.0,
                    strategy_usage=strategy_usage,
                    last_updated=datetime.now().isoformat()
                )
            else:
                stats = MappingStats(0, 0, 0, 0.0, 0.0, {}, datetime.now().isoformat())
                
            conn.close()
            return stats
            
        except Exception as e:
            logger.warning(f"Could not load mapping stats: {e}")
            return MappingStats(0, 0, 0, 0.0, 0.0, {}, datetime.now().isoformat())
    
    def normalize_team_name(self, name: str) -> str:
        """Normalize team name for better matching"""
        if not name:
            return ""
        
        normalized = name.strip()
        
        # Apply normalization rules
        for pattern, replacement in self.normalization_rules.items():
            normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
        
        # Clean up extra spaces and convert to lowercase
        normalized = re.sub(r'\s+', ' ', normalized).strip().lower()
        
        return normalized
    
    def find_team_mapping(self, api_football_name: str, odds_api_teams: List[str], 
                         league_context: str = None) -> MappingResult:
        """
        Find the best matching team name using multiple strategies
        
        Args:
            api_football_name: Team name from API-Football
            odds_api_teams: List of available team names from Odds API
            league_context: League name for context-based matching
            
        Returns:
            MappingResult with best match and metadata
        """
        start_time = datetime.now()
        
        # Strategy 1: Exact Match
        result = self._strategy_exact_match(api_football_name, odds_api_teams)
        if result.match_found and result.confidence >= 1.0:
            result.processing_time = (datetime.now() - start_time).total_seconds()
            self._record_mapping_attempt(result, league_context)
            return result
        
        # Strategy 2: Manual Mapping Table
        result = self._strategy_manual_mapping(api_football_name, odds_api_teams)
        if result.match_found and result.confidence >= 0.95:
            result.processing_time = (datetime.now() - start_time).total_seconds()
            self._record_mapping_attempt(result, league_context)
            return result
        
        # Strategy 3: Learned Mappings
        result = self._strategy_learned_mapping(api_football_name, odds_api_teams)
        if result.match_found and result.confidence >= 0.9:
            result.processing_time = (datetime.now() - start_time).total_seconds()
            self._record_mapping_attempt(result, league_context)
            return result
        
        # Strategy 4: Normalized String Matching
        result = self._strategy_normalized_matching(api_football_name, odds_api_teams)
        if result.match_found and result.confidence >= 0.85:
            result.processing_time = (datetime.now() - start_time).total_seconds()
            self._record_mapping_attempt(result, league_context)
            return result
        
        # Strategy 5: Substring Matching
        result = self._strategy_substring_matching(api_football_name, odds_api_teams)
        if result.match_found and result.confidence >= 0.75:
            result.processing_time = (datetime.now() - start_time).total_seconds()
            self._record_mapping_attempt(result, league_context)
            return result
        
        # Strategy 6: Word-based Matching
        result = self._strategy_word_based_matching(api_football_name, odds_api_teams)
        if result.match_found and result.confidence >= 0.7:
            result.processing_time = (datetime.now() - start_time).total_seconds()
            self._record_mapping_attempt(result, league_context)
            return result
        
        # Strategy 7: Fuzzy String Matching
        result = self._strategy_fuzzy_matching(api_football_name, odds_api_teams)
        if result.match_found and result.confidence >= 0.6:
            result.processing_time = (datetime.now() - start_time).total_seconds()
            self._record_mapping_attempt(result, league_context)
            return result
        
        # No match found - return best attempt from fuzzy matching
        result.processing_time = (datetime.now() - start_time).total_seconds()
        self._record_mapping_attempt(result, league_context)
        return result
    
    def _strategy_exact_match(self, api_name: str, odds_teams: List[str]) -> MappingResult:
        """Strategy 1: Exact string matching"""
        for odds_name in odds_teams:
            if api_name == odds_name:
                return MappingResult(
                    api_football_name=api_name,
                    odds_api_name=odds_name,
                    confidence=1.0,
                    strategy_used="exact_match",
                    match_found=True,
                    alternatives=[],
                    processing_time=0.0
                )
        
        return MappingResult(
            api_football_name=api_name,
            odds_api_name="",
            confidence=0.0,
            strategy_used="exact_match",
            match_found=False,
            alternatives=[],
            processing_time=0.0
        )
    
    def _strategy_manual_mapping(self, api_name: str, odds_teams: List[str]) -> MappingResult:
        """Strategy 2: Manual mapping table lookup"""
        mapped_name = self.manual_mappings.get(api_name)
        
        if mapped_name and mapped_name in odds_teams:
            return MappingResult(
                api_football_name=api_name,
                odds_api_name=mapped_name,
                confidence=0.95,
                strategy_used="manual_mapping",
                match_found=True,
                alternatives=[],
                processing_time=0.0
            )
        
        return MappingResult(
            api_football_name=api_name,
            odds_api_name="",
            confidence=0.0,
            strategy_used="manual_mapping",
            match_found=False,
            alternatives=[],
            processing_time=0.0
        )
    
    def _strategy_learned_mapping(self, api_name: str, odds_teams: List[str]) -> MappingResult:
        """Strategy 3: Previously learned mappings"""
        learned_name = self.learned_mappings.get(api_name)
        
        if learned_name and learned_name in odds_teams:
            return MappingResult(
                api_football_name=api_name,
                odds_api_name=learned_name,
                confidence=0.9,
                strategy_used="learned_mapping",
                match_found=True,
                alternatives=[],
                processing_time=0.0
            )
        
        return MappingResult(
            api_football_name=api_name,
            odds_api_name="",
            confidence=0.0,
            strategy_used="learned_mapping",
            match_found=False,
            alternatives=[],
            processing_time=0.0
        )
    
    def _strategy_normalized_matching(self, api_name: str, odds_teams: List[str]) -> MappingResult:
        """Strategy 4: Normalized string matching"""
        normalized_api = self.normalize_team_name(api_name)
        
        best_match = ""
        best_confidence = 0.0
        
        for odds_name in odds_teams:
            normalized_odds = self.normalize_team_name(odds_name)
            
            if normalized_api == normalized_odds:
                confidence = 0.85
                if confidence > best_confidence:
                    best_match = odds_name
                    best_confidence = confidence
        
        return MappingResult(
            api_football_name=api_name,
            odds_api_name=best_match,
            confidence=best_confidence,
            strategy_used="normalized_matching",
            match_found=best_confidence > 0,
            alternatives=[],
            processing_time=0.0
        )
    
    def _strategy_substring_matching(self, api_name: str, odds_teams: List[str]) -> MappingResult:
        """Strategy 5: Substring matching"""
        normalized_api = self.normalize_team_name(api_name)
        
        best_match = ""
        best_confidence = 0.0
        alternatives = []
        
        for odds_name in odds_teams:
            normalized_odds = self.normalize_team_name(odds_name)
            
            # Check if API name contains odds name or vice versa
            if normalized_api in normalized_odds or normalized_odds in normalized_api:
                # Calculate confidence based on length ratio
                if len(normalized_api) > 0:
                    overlap = min(len(normalized_api), len(normalized_odds))
                    total = max(len(normalized_api), len(normalized_odds))
                    confidence = (overlap / total) * 0.75
                    
                    if confidence > best_confidence:
                        if best_match:
                            alternatives.append(best_match)
                        best_match = odds_name
                        best_confidence = confidence
                    elif confidence > 0.5:
                        alternatives.append(odds_name)
        
        return MappingResult(
            api_football_name=api_name,
            odds_api_name=best_match,
            confidence=best_confidence,
            strategy_used="substring_matching",
            match_found=best_confidence > 0,
            alternatives=alternatives[:3],  # Limit alternatives
            processing_time=0.0
        )
    
    def _strategy_word_based_matching(self, api_name: str, odds_teams: List[str]) -> MappingResult:
        """Strategy 6: Word-based matching"""
        api_words = set(self.normalize_team_name(api_name).split())
        
        best_match = ""
        best_confidence = 0.0
        alternatives = []
        
        for odds_name in odds_teams:
            odds_words = set(self.normalize_team_name(odds_name).split())
            
            if api_words and odds_words:
                # Calculate Jaccard similarity
                intersection = len(api_words.intersection(odds_words))
                union = len(api_words.union(odds_words))
                
                if union > 0:
                    jaccard_similarity = intersection / union
                    confidence = jaccard_similarity * 0.7
                    
                    if confidence > best_confidence and confidence > 0.3:
                        if best_match:
                            alternatives.append(best_match)
                        best_match = odds_name
                        best_confidence = confidence
                    elif confidence > 0.3:
                        alternatives.append(odds_name)
        
        return MappingResult(
            api_football_name=api_name,
            odds_api_name=best_match,
            confidence=best_confidence,
            strategy_used="word_based_matching",
            match_found=best_confidence > 0,
            alternatives=alternatives[:3],
            processing_time=0.0
        )
    
    def _strategy_fuzzy_matching(self, api_name: str, odds_teams: List[str]) -> MappingResult:
        """Strategy 7: Fuzzy string matching using difflib"""
        normalized_api = self.normalize_team_name(api_name)
        
        best_matches = []
        
        for odds_name in odds_teams:
            normalized_odds = self.normalize_team_name(odds_name)
            
            # Use difflib's sequence matcher
            similarity = difflib.SequenceMatcher(None, normalized_api, normalized_odds).ratio()
            
            if similarity > 0.4:  # Minimum threshold
                best_matches.append((odds_name, similarity))
        
        # Sort by similarity
        best_matches.sort(key=lambda x: x[1], reverse=True)
        
        if best_matches:
            best_match, similarity = best_matches[0]
            confidence = similarity * 0.6  # Scale down for fuzzy matching
            alternatives = [match[0] for match in best_matches[1:4]]  # Top 3 alternatives
            
            return MappingResult(
                api_football_name=api_name,
                odds_api_name=best_match,
                confidence=confidence,
                strategy_used="fuzzy_matching",
                match_found=confidence >= 0.3,
                alternatives=alternatives,
                processing_time=0.0
            )
        
        return MappingResult(
            api_football_name=api_name,
            odds_api_name="",
            confidence=0.0,
            strategy_used="fuzzy_matching",
            match_found=False,
            alternatives=[],
            processing_time=0.0
        )
    
    def _record_mapping_attempt(self, result: MappingResult, league_context: str = None):
        """Record mapping attempt in database for learning and statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                INSERT INTO mapping_attempts 
                (api_football_name, odds_api_name, confidence, strategy_used, success, 
                 processing_time, alternatives, league_context)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result.api_football_name,
                result.odds_api_name if result.match_found else None,
                result.confidence,
                result.strategy_used,
                result.match_found,
                result.processing_time,
                json.dumps(result.alternatives),
                league_context
            ))
            
            # If high confidence match and learning enabled, store as learned mapping
            if (self.learn_mappings and result.match_found and 
                result.confidence >= 0.8 and result.strategy_used != "learned_mapping"):
                
                conn.execute("""
                    INSERT OR REPLACE INTO team_mappings 
                    (api_football_name, odds_api_name, confidence, strategy_used, league_context)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    result.api_football_name,
                    result.odds_api_name,
                    result.confidence,
                    result.strategy_used,
                    league_context
                ))
                
                # Update learned mappings cache
                self.learned_mappings[result.api_football_name] = result.odds_api_name
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to record mapping attempt: {e}")
    
    def get_mapping_report(self, days: int = 7) -> Dict[str, Any]:
        """Generate mapping performance report"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            
            # Get statistics for the last N days
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            # Overall statistics
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_attempts,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(CASE WHEN success = 1 THEN confidence END) as avg_confidence,
                    AVG(processing_time) as avg_processing_time
                FROM mapping_attempts 
                WHERE attempted_at >= ?
            """, (cutoff_date,))
            
            stats = dict(cursor.fetchone())
            success_rate = (stats['successful'] / stats['total_attempts'] 
                          if stats['total_attempts'] > 0 else 0.0)
            
            # Strategy performance
            cursor = conn.execute("""
                SELECT strategy_used, 
                       COUNT(*) as attempts,
                       SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successes,
                       AVG(CASE WHEN success = 1 THEN confidence END) as avg_confidence
                FROM mapping_attempts 
                WHERE attempted_at >= ?
                GROUP BY strategy_used
                ORDER BY successes DESC
            """, (cutoff_date,))
            
            strategy_stats = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                row_dict['success_rate'] = (row_dict['successes'] / row_dict['attempts'] 
                                          if row_dict['attempts'] > 0 else 0.0)
                strategy_stats.append(row_dict)
            
            # Failed mappings for review
            cursor = conn.execute("""
                SELECT api_football_name, alternatives, league_context, COUNT(*) as failure_count
                FROM mapping_attempts 
                WHERE success = 0 AND attempted_at >= ?
                GROUP BY api_football_name, alternatives, league_context
                ORDER BY failure_count DESC
                LIMIT 20
            """, (cutoff_date,))
            
            failed_mappings = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                row_dict['alternatives'] = json.loads(row_dict['alternatives'] or '[]')
                failed_mappings.append(row_dict)
            
            # Recent successful mappings
            cursor = conn.execute("""
                SELECT api_football_name, odds_api_name, confidence, strategy_used, 
                       attempted_at, league_context
                FROM mapping_attempts 
                WHERE success = 1 AND attempted_at >= ?
                ORDER BY attempted_at DESC
                LIMIT 10
            """, (cutoff_date,))
            
            recent_successes = [dict(row) for row in cursor.fetchall()]
            
            conn.close()
            
            report = {
                'report_date': datetime.now().isoformat(),
                'period_days': days,
                'overall_stats': {
                    'total_attempts': stats['total_attempts'],
                    'successful_mappings': stats['successful'],
                    'success_rate': success_rate,
                    'avg_confidence': stats['avg_confidence'] or 0.0,
                    'avg_processing_time': stats['avg_processing_time'] or 0.0
                },
                'strategy_performance': strategy_stats,
                'failed_mappings': failed_mappings,
                'recent_successes': recent_successes,
                'learned_mappings_count': len(self.learned_mappings),
                'manual_mappings_count': len(self.manual_mappings)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate mapping report: {e}")
            return {}
    
    def verify_mapping(self, api_football_name: str, odds_api_name: str, 
                      is_correct: bool, league_context: str = None):
        """Manually verify a mapping result for learning"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            if is_correct:
                # Add to verified mappings
                conn.execute("""
                    INSERT OR REPLACE INTO team_mappings 
                    (api_football_name, odds_api_name, confidence, strategy_used, 
                     verified, league_context)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (api_football_name, odds_api_name, 1.0, "manual_verification", 
                      1, league_context))
                
                # Update learned mappings cache
                self.learned_mappings[api_football_name] = odds_api_name
                
            else:
                # Mark as incorrect to avoid future suggestions
                conn.execute("""
                    DELETE FROM team_mappings 
                    WHERE api_football_name = ? AND odds_api_name = ?
                """, (api_football_name, odds_api_name))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Mapping verification recorded: {api_football_name} -> {odds_api_name} ({'correct' if is_correct else 'incorrect'})")
            
        except Exception as e:
            logger.error(f"Failed to verify mapping: {e}")

def collect_odds_data_enhanced(data: Dict, phase: str, mapper: EnhancedTeamMapper = None):
    """
    Enhanced version of collect_odds_data with intelligent team mapping
    
    This replaces the existing collect_odds_data function in the workflow
    """
    try:
        import requests
        import os
        
        if not mapper:
            mapper = EnhancedTeamMapper()
            
        odds_api_key = os.getenv('ODDS_API_KEY')
        if not odds_api_key:
            print("  ⚠️ No Odds API key configured")
            return
        
        game_info = data['game_info']
        league_name = game_info['league']
        
        # Enhanced league mapping with more coverage
        odds_sports_map = {
            'Premier League': 'soccer_epl',
            'La Liga': 'soccer_spain_la_liga', 
            'Bundesliga': 'soccer_germany_bundesliga',
            'Serie A': 'soccer_italy_serie_a',
            'Ligue 1': 'soccer_france_ligue_one',
            'Champions League': 'soccer_uefa_champs_league',
            'Europa League': 'soccer_uefa_europa_league',
            'Copa Libertadores': 'soccer_conmebol_copa_libertadores',
            'Brasileirão Serie A': 'soccer_brazil_campeonato',
            'Liga Profesional Argentina': 'soccer_argentina_primera_division',
            'Eredivisie': 'soccer_netherlands_eredivisie',
            'Primeira Liga': 'soccer_portugal_primeira_liga',
            'Championship': 'soccer_efl_champ',
            'MLS': 'soccer_usa_mls',
            'Liga MX': 'soccer_mexico_ligamx'
        }
        
        sport = odds_sports_map.get(league_name)
        if not sport:
            print(f"  ⚠️ No odds mapping for {league_name}")
            return
        
        odds_url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/"
        odds_params = {
            'apiKey': odds_api_key,
            'regions': 'eu,us,au',
            'markets': 'h2h,spreads,totals',
            'oddsFormat': 'decimal',
            'dateFormat': 'iso'
        }
        
        response = requests.get(odds_url, params=odds_params, timeout=15)
        if response.status_code == 200:
            odds_data = response.json()
            
            # Extract available team names from odds API
            available_teams = []
            for game in odds_data:
                if game.get('home_team'):
                    available_teams.append(game['home_team'])
                if game.get('away_team'):
                    available_teams.append(game['away_team'])
            
            # Use enhanced mapping for home team
            home_result = mapper.find_team_mapping(
                game_info['home_team'],
                available_teams,
                league_context=league_name
            )
            
            # Use enhanced mapping for away team  
            away_result = mapper.find_team_mapping(
                game_info['away_team'],
                available_teams,
                league_context=league_name
            )
            
            print(f"  🎯 Home team mapping: {home_result.api_football_name} -> {home_result.odds_api_name} (confidence: {home_result.confidence:.2f}, strategy: {home_result.strategy_used})")
            print(f"  🎯 Away team mapping: {away_result.api_football_name} -> {away_result.odds_api_name} (confidence: {away_result.confidence:.2f}, strategy: {away_result.strategy_used})")
            
            # Find matching game using enhanced mappings
            matching_game = None
            for game in odds_data:
                game_home = game.get('home_team', '')
                game_away = game.get('away_team', '')
                
                home_match = (home_result.match_found and 
                             home_result.odds_api_name == game_home)
                away_match = (away_result.match_found and 
                             away_result.odds_api_name == game_away)
                
                if home_match and away_match:
                    matching_game = game
                    # Store additional mapping metadata
                    matching_game['_mapping_metadata'] = {
                        'home_mapping': asdict(home_result),
                        'away_mapping': asdict(away_result)
                    }
                    break
            
            if matching_game:
                data['data'][f'odds_{phase}'] = matching_game
                print(f"  ✅ Odds collected for {phase} with enhanced mapping")
            else:
                print(f"  ⚠️ No matching game found despite enhanced mapping attempts")
                
                # Store mapping attempts for analysis
                data['data'][f'mapping_attempts_{phase}'] = {
                    'home_mapping': asdict(home_result),
                    'away_mapping': asdict(away_result),
                    'available_teams': available_teams[:10]  # Limit for storage
                }
                
        elif response.status_code == 429:
            print(f"  ⚠️ Odds API rate limited")
        else:
            print(f"  ⚠️ Odds API error: {response.status_code}")
            
    except Exception as e:
        print(f"  ⚠️ Enhanced odds collection error: {e}")

# CLI and testing utilities
if __name__ == "__main__":
    # Test the enhanced mapping system
    mapper = EnhancedTeamMapper()
    
    # Test cases
    test_cases = [
        ("Manchester United", ["Manchester Utd", "Manchester City", "Liverpool"]),
        ("FC Barcelona", ["Barcelona", "Real Madrid", "Atletico Madrid"]),
        ("Bayern Munich", ["Bayern Munich", "Dortmund", "RB Leipzig"]),
        ("Borussia Dortmund", ["Dortmund", "Bayern Munich", "Schalke"]),
        ("Paris Saint Germain", ["PSG", "Marseille", "Lyon"])
    ]
    
    print("🧪 Testing Enhanced Team Mapping")
    print("=" * 50)
    
    for api_name, odds_teams in test_cases:
        result = mapper.find_team_mapping(api_name, odds_teams, "Test League")
        
        print(f"\n📊 API Team: {api_name}")
        print(f"   Available: {odds_teams}")
        print(f"   ✅ Best Match: {result.odds_api_name}")
        print(f"   📈 Confidence: {result.confidence:.3f}")
        print(f"   🛠️ Strategy: {result.strategy_used}")
        print(f"   ⏱️ Time: {result.processing_time:.3f}s")
        if result.alternatives:
            print(f"   🔄 Alternatives: {result.alternatives}")
    
    # Generate and display mapping report
    print("\n📋 Mapping Performance Report")
    print("=" * 50)
    report = mapper.get_mapping_report(days=1)  # Today's activity
    
    if report:
        overall = report['overall_stats']
        print(f"Total Attempts: {overall['total_attempts']}")
        print(f"Success Rate: {overall['success_rate']:.1%}")
        print(f"Avg Confidence: {overall['avg_confidence']:.3f}")
        print(f"Avg Processing Time: {overall['avg_processing_time']:.3f}s")
        
        print("\n🏆 Strategy Performance:")
        for strategy in report['strategy_performance']:
            print(f"  {strategy['strategy_used']}: {strategy['success_rate']:.1%} "
                  f"({strategy['successes']}/{strategy['attempts']})")
    
    print("\n✅ Enhanced mapping system ready for integration!")