"""
Script to populate vector database with configurable settings
"""

import sys
import os
from datetime import datetime, timedelta
import argparse

# Setup path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from data.data import ClickhouseHelper, Interval
from backend.embedding_service import embedding_service, EmbeddingConfig, CandleInterval
from tqdm import tqdm


def populate_kline_data(
    ticker: str = "BTCUSDT", 
    days: int = 30,
    resample_to: str = "1h",
    batch_size: int = 100
):
    """
    Populate vector DB with OHLCV data
    
    Args:
        ticker: Ticker symbol
        days: Number of days to fetch
        resample_to: Candle interval ('5m', '1h', '4h', '1d')
        batch_size: Records per batch
    """
    print(f"\n{'='*60}")
    print(f"Populating KLINE data for {ticker}")
    print(f"Period: {days} days | Resample to: {resample_to}")
    print(f"{'='*60}")
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    # Fetch data từ ClickHouse (luôn lấy 5m, sau đó resample)
    df = ClickhouseHelper.get_data_between(
        ticker=ticker,
        time_start=start_time,
        time_end=end_time,
        interval=Interval.FIVE_MINUTES,
        verbose=True
    )
    
    if df.empty:
        print(f"[E] No data found for {ticker}")
        return
    
    print(f"Fetched {len(df)} records (5m candles)")
    
    # Convert to list of dicts
    data_list = df.to_dict('records')
    for record in data_list:
        record['ticker'] = ticker
    
    # Add to vector store với resampling
    embedding_service.add_kline_data(data_list, resample_to=resample_to)
    
    print(f"✓ Completed KLINE for {ticker}")


def populate_news_data(days: int = 30, batch_size: int = 50):
    """Populate vector DB with news data"""
    print(f"\n{'='*60}")
    print(f"Populating NEWS data (last {days} days)")
    print(f"{'='*60}")
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    df = ClickhouseHelper.get_news_between(
        time_start=start_time,
        time_end=end_time,
        verbose=True,
        include_body=True
    )
    
    if df.empty:
        print("⚠️ No news data found")
        return
    
    print(f"Processing {len(df)} news articles...")
    
    news_list = df.to_dict('records')
    
    for i in tqdm(range(0, len(news_list), batch_size), desc="Adding to vector store"):
        batch = news_list[i:i+batch_size]
        embedding_service.add_news_data(batch)
    
    print(f"✓ Added {len(news_list)} news articles")


def populate_open_interest_data(symbol: str = "BTCUSDT", days: int = 30, batch_size: int = 100):
    """Populate vector DB with open interest data"""
    print(f"\n{'='*60}")
    print(f"Populating OPEN INTEREST data for {symbol} (last {days} days)")
    print(f"{'='*60}")
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    query = f"""
            SELECT 
                symbol,
                sumOpenInterest,
                sumOpenInterestValue,
                CMCCirculatingSupply,
                timestamp
            FROM open_interest_history_5m
            WHERE symbol = '{symbol}'
                AND timestamp >= {int(start_time.timestamp())}
                AND timestamp <= {int(end_time.timestamp())}
            ORDER BY timestamp
            """
    
    df = ClickhouseHelper.run_to_df(query, verbose=True)
    
    if df.empty:
        print(f"⚠️ No open interest data found for {symbol}")
        return
    
    print(f"Processing {len(df)} records...")
    
    oi_list = df.to_dict('records')
    
    for i in tqdm(range(0, len(oi_list), batch_size), desc="Adding to vector store"):
        batch = oi_list[i:i+batch_size]
        embedding_service.add_open_interest_data(batch)
    
    print(f"✓ Added {len(oi_list)} open interest records")


def populate_analysis_examples():
    """Add analysis guides"""
    print(f"\n{'='*60}")
    print("Adding analysis examples")
    print(f"{'='*60}")
    
    examples = [
        {
            "text": """RSI (Relative Strength Index) Analysis:
            - RSI > 70: Overbought condition, potential selling pressure
            - RSI < 30: Oversold condition, potential buying opportunity
            - RSI 40-60: Neutral zone, no strong signal
            - Divergence between price and RSI can signal trend reversal""",
            "metadata": {"indicator": "RSI", "type": "guide"}
        },
        {
            "text": """MACD (Moving Average Convergence Divergence) Analysis:
            - MACD crosses above signal line: Bullish signal
            - MACD crosses below signal line: Bearish signal
            - Histogram expanding: Trend strengthening
            - Histogram contracting: Trend weakening""",
            "metadata": {"indicator": "MACD", "type": "guide"}
        },
        {
            "text": """Bollinger Bands Analysis:
            - Price touching upper band: Potential overbought
            - Price touching lower band: Potential oversold
            - Bands narrowing (squeeze): Volatility about to increase
            - Bands widening: High volatility period
            - Price breaking out of bands: Strong trend""",
            "metadata": {"indicator": "Bollinger Bands", "type": "guide"}
        },
        {
            "text": """Volume Analysis:
            - Rising volume with rising price: Confirms uptrend
            - Rising volume with falling price: Confirms downtrend
            - Decreasing volume: Trend may be weakening
            - Volume spike: Significant market event or news""",
            "metadata": {"indicator": "Volume", "type": "guide"}
        },
        {
            "text": """Market Sentiment Analysis:
            - Positive news sentiment: May drive prices up
            - Negative news sentiment: May drive prices down
            - Mixed sentiment: Market uncertainty
            - High news volume: Increased market attention""",
            "metadata": {"type": "sentiment", "topic": "news"}
        }
    ]
    
    for idx, example in enumerate(examples):
        embedding_service.add_analysis_result(
            analysis=example["text"],
            metadata=example["metadata"],
            doc_id=f"analysis_guide_{idx}"
        )
    
    print(f"✓ Added {len(examples)} analysis examples")


def main():
    parser = argparse.ArgumentParser(
        description="Populate vector database with configurable settings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            # Mặc định: KLINE(30d,1h) + NEWS(30d) + OI(30d) + Analysis
            python data/populate_vectordb.py --ticker BTCUSDT
            
            # Tùy chỉnh số ngày cho từng loại
            python data/populate_vectordb.py --ticker BTCUSDT --kline-days 90 --news-days 7 --oi-days 60
            
            # Chỉ nạp NEWS
            python data/populate_vectordb.py --skip-kline --skip-oi --news-days 60
            
            # Nến 4h thay vì 1h
            python data/populate_vectordb.py --ticker BTCUSDT --kline-interval 4h
            
            # Reset và nạp lại
            python data/populate_vectordb.py --reset --ticker ETHUSDT
            """
    )
    
    # General options
    parser.add_argument("--reset", action="store_true", help="Reset all collections before populating")
    parser.add_argument("--ticker", type=str, default="BTCUSDT", help="Ticker symbol (default: BTCUSDT)")
    
    # KLINE options
    parser.add_argument("--skip-kline", action="store_true", help="Skip KLINE data")
    parser.add_argument("--kline-days", type=int, default=30, help="Days to fetch for KLINE (default: 30)")
    parser.add_argument("--kline-interval", type=str, default="1h", 
                       choices=['5m', '15m', '1h', '4h', '1d'],
                       help="Candle interval for KLINE (default: 1h)")
    
    # NEWS options
    parser.add_argument("--skip-news", action="store_true", help="Skip NEWS data")
    parser.add_argument("--news-days", type=int, default=30, help="Days to fetch for NEWS (default: 30)")
    
    # OPEN INTEREST options
    parser.add_argument("--skip-oi", action="store_true", help="Skip OPEN INTEREST data")
    parser.add_argument("--oi-days", type=int, default=30, help="Days to fetch for OPEN INTEREST (default: 30)")
    
    # ANALYSIS options
    parser.add_argument("--skip-analysis", action="store_true", help="Skip ANALYSIS guides")
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("VECTOR DATABASE POPULATION SCRIPT")
    print("="*60)
    print(f"Ticker: {args.ticker}")
    print(f"KLINE: {'SKIP' if args.skip_kline else f'{args.kline_days} days, {args.kline_interval}'}")
    print(f"NEWS: {'SKIP' if args.skip_news else f'{args.news_days} days'}")
    print(f"OPEN INTEREST: {'SKIP' if args.skip_oi else f'{args.oi_days} days'}")
    print(f"ANALYSIS: {'SKIP' if args.skip_analysis else 'YES'}")
    print("="*60)
    
    # Reset if requested
    if args.reset:
        print("\n⚠️  Resetting all collections...")
        embedding_service.reset_all()
    
    # Populate data
    try:
        if not args.skip_kline:
            populate_kline_data(
                ticker=args.ticker, 
                days=args.kline_days,
                resample_to=args.kline_interval
            )
        
        if not args.skip_news:
            populate_news_data(days=args.news_days)
        
        if not args.skip_oi:
            populate_open_interest_data(symbol=args.ticker, days=args.oi_days)
        
        if not args.skip_analysis:
            populate_analysis_examples()
        
        # Show stats
        print("\n" + "="*60)
        print("VECTOR DATABASE STATISTICS")
        print("="*60)
        stats = embedding_service.get_stats()
        for collection, info in stats.items():
            status = "✓" if info['enabled'] else "✗"
            print(f"{status} {collection.upper()}: {info['count']:,} records")
        
        print("\n" + "="*60)
        print("✓ POPULATION COMPLETE!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Error during population: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()