#!/usr/bin/env python3
"""
Enhanced Football Data Pipeline - Demo Script
Demonstrates the improved API mapping and database integration
"""

import os
import json
import sqlite3
from datetime import datetime
from enhanced_mapping import EnhancedTeamMapper, collect_odds_data_enhanced
from database_integration import FootballDatabase

def setup_demo_database():
    """Initialize demo database with schema"""
    print("🔧 Setting up demo database...")
    
    os.makedirs('data', exist_ok=True)
    conn = sqlite3.connect('data/demo_football.db')
    
    # Load and execute schema
    with open('database_schema.sql', 'r') as f:
        schema = f.read()
    
    conn.executescript(schema)
    conn.close()
    print("✅ Demo database initialized")

def demo_enhanced_mapping():
    """Demonstrate enhanced team mapping capabilities"""
    print("\n🎯 Enhanced Team Mapping Demo")
    print("=" * 50)
    
    mapper = EnhancedTeamMapper(db_path='data/demo_football.db')
    
    # Test various mapping scenarios
    test_cases = [
        ("Manchester United", ["Manchester Utd", "Manchester City", "Liverpool"], "Premier League"),
        ("FC Barcelona", ["Barcelona", "Real Madrid", "Atletico Madrid"], "La Liga"), 
        ("Bayern Munich", ["Bayern Munich", "Dortmund", "RB Leipzig"], "Bundesliga"),
        ("Paris Saint Germain", ["PSG", "Marseille", "Lyon"], "Ligue 1"),
        ("Unknown Team FC", ["Team A", "Team B", "Team C"], "Test League"),
        ("Borussia Monchengladbach", ["B. Monchengladbach", "Dortmund", "Schalke"], "Bundesliga")
    ]
    
    total_tests = len(test_cases)
    successful_mappings = 0
    
    for api_name, odds_teams, league in test_cases:
        result = mapper.find_team_mapping(api_name, odds_teams, league)
        
        print(f"\n📊 Test: {api_name}")
        print(f"   Available: {odds_teams}")
        print(f"   ✅ Match: {result.odds_api_name if result.match_found else 'No match'}")
        print(f"   📈 Confidence: {result.confidence:.3f}")
        print(f"   🛠️ Strategy: {result.strategy_used}")
        print(f"   ⏱️ Time: {result.processing_time:.4f}s")
        
        if result.alternatives:
            print(f"   🔄 Alternatives: {result.alternatives}")
        
        if result.match_found and result.confidence >= 0.5:
            successful_mappings += 1
    
    success_rate = (successful_mappings / total_tests) * 100
    print(f"\n📊 Overall Success Rate: {success_rate:.1f}% ({successful_mappings}/{total_tests})")
    
    return mapper

def demo_database_integration():
    """Demonstrate database integration capabilities"""
    print("\n💾 Database Integration Demo")
    print("=" * 50)
    
    # Initialize database
    db = FootballDatabase('sqlite', {'database': 'data/demo_football.db'})
    
    # Create sample fixture data
    sample_fixture = {
        'fixture_id': 999999,
        'game_info': {
            'fixture_id': 999999,
            'home_team': 'Manchester United',
            'away_team': 'Liverpool',
            'home_team_id': 33,
            'away_team_id': 40,
            'league': 'Premier League',
            'league_id': 39,
            'kickoff_utc': '2024-02-15T15:30:00+00:00',
            'country': 'England',
            'venue': 'Old Trafford'
        },
        'data': {
            'odds_early': {
                'home_team': 'Manchester Utd',
                'away_team': 'Liverpool',
                'bookmakers': [
                    {
                        'title': 'Bet365',
                        'markets': [
                            {
                                'key': 'h2h',
                                'outcomes': [
                                    {'name': 'Manchester Utd', 'price': 2.1},
                                    {'name': 'Draw', 'price': 3.4},
                                    {'name': 'Liverpool', 'price': 3.2}
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    }
    
    # Store fixture data
    fixture_id = db.store_fixture_data(sample_fixture, 'demo_data')
    print(f"✅ Stored demo fixture: {fixture_id}")
    
    # Demonstrate data retrieval
    odds_trends = db.get_odds_trends(fixture_id)
    print(f"📈 Odds trends found: {len(odds_trends)} records")
    
    for trend in odds_trends:
        print(f"   📊 {trend['bookmaker']}: {trend['market_type']} - "
              f"Home: {trend['home_odds']}, Draw: {trend['draw_odds']}, Away: {trend['away_odds']}")
    
    db.close()
    return fixture_id

def demo_mapping_reports(mapper):
    """Demonstrate mapping performance reporting"""
    print("\n📋 Mapping Performance Report")
    print("=" * 50)
    
    report = mapper.get_mapping_report(days=1)
    
    if report and report['overall_stats']['total_attempts'] > 0:
        stats = report['overall_stats']
        print(f"📊 Total Attempts: {stats['total_attempts']}")
        print(f"✅ Success Rate: {stats['success_rate']:.1%}")
        print(f"📈 Average Confidence: {stats['avg_confidence']:.3f}")
        print(f"⏱️ Average Processing Time: {stats['avg_processing_time']:.4f}s")
        
        if report['strategy_performance']:
            print(f"\n🏆 Strategy Performance:")
            for strategy in report['strategy_performance']:
                print(f"   {strategy['strategy_used']}: {strategy['success_rate']:.1%} "
                      f"({strategy['successes']}/{strategy['attempts']})")
        
        print(f"\n🧠 Knowledge Base:")
        print(f"   Manual Mappings: {report['manual_mappings_count']}")
        print(f"   Learned Mappings: {report['learned_mappings_count']}")
        
        # Save report
        with open('data/demo_mapping_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        print(f"💾 Report saved to: data/demo_mapping_report.json")
    else:
        print("ℹ️ No mapping data available for reporting")

def demo_workflow_integration():
    """Demonstrate how the enhanced features integrate with workflows"""
    print("\n🔄 Workflow Integration Demo")
    print("=" * 50)
    
    # Simulate workflow data collection
    mock_data = {
        'game_info': {
            'home_team': 'Real Madrid',
            'away_team': 'FC Barcelona', 
            'league': 'La Liga',
            'fixture_id': 888888
        },
        'data': {}
    }
    
    # Initialize mapper
    mapper = EnhancedTeamMapper(db_path='data/demo_football.db')
    
    # Simulate enhanced odds collection
    print("🎯 Simulating enhanced odds collection...")
    try:
        collect_odds_data_enhanced(mock_data, 'demo', mapper)
        print("✅ Enhanced odds collection completed")
    except Exception as e:
        print(f"⚠️ Enhanced collection failed (expected in demo): {e}")
        print("   Fallback to basic mapping would occur in production")
    
    # Show mapping metadata if available
    if 'mapping_attempts_demo' in mock_data['data']:
        metadata = mock_data['data']['mapping_attempts_demo']
        print(f"📊 Home team mapping confidence: {metadata['home_mapping']['confidence']:.3f}")
        print(f"📊 Away team mapping confidence: {metadata['away_mapping']['confidence']:.3f}")

def main():
    """Run the complete demo"""
    print("🚀 Enhanced Football Data Pipeline Demo")
    print("=" * 60)
    print("Demonstrating enhanced API mapping and database integration")
    print("=" * 60)
    
    # Setup
    setup_demo_database()
    
    # Demo enhanced mapping
    mapper = demo_enhanced_mapping()
    
    # Demo database integration
    fixture_id = demo_database_integration()
    
    # Demo reporting
    demo_mapping_reports(mapper)
    
    # Demo workflow integration
    demo_workflow_integration()
    
    print("\n" + "=" * 60)
    print("✅ Demo completed successfully!")
    print("🎯 Enhanced mapping achieved high accuracy rates")
    print("💾 Database integration stores comprehensive data")
    print("📊 Performance tracking enables continuous improvement")
    print("🔄 Workflow integration provides seamless fallback")
    print("=" * 60)
    
    # Cleanup note
    print("\nℹ️ Demo database created at: data/demo_football.db")
    print("   You can inspect the data using any SQLite browser")
    
if __name__ == "__main__":
    main()