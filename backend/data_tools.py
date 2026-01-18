import pandas as pd
from datetime import datetime, timedelta
from data.data import ClickhouseHelper, Interval
from functools import lru_cache
from typing import Optional, Dict, Any


class DataTools:
    
    @staticmethod
    @lru_cache(maxsize=1000)
    def get_exact_price(ticker: str, date_str: str) -> Dict[str, Any]:
        """
        Lấy giá chính xác cho một ngày cụ thể
        
        Args:
            ticker: Mã coin (e.g. BTCUSDT)
            date_str: Ngày (YYYY-MM-DD)
            
        Returns:
            Dict với status, source, data, formatted
        """
        try:
            target_date = pd.to_datetime(date_str)
            start_ts = target_date.replace(hour=0, minute=0, second=0)
            end_ts = target_date.replace(hour=23, minute=59, second=59)

            df = ClickhouseHelper.get_data_between(
                ticker=ticker,
                time_start=start_ts,
                time_end=end_ts,
                interval=Interval.FIVE_MINUTES,
                verbose=False
            )

            if df.empty:
                return {
                    'status': 'no_data',
                    'source': 'database',
                    'message': f"Database không có dữ liệu cho {ticker} vào {date_str}",
                    'suggestion': 'Thử chọn ngày gần hơn hoặc ticker khác',
                    'query_info': {
                        'ticker': ticker,
                        'date': date_str,
                        'type': 'exact_price'
                    }
                }

            # Convert Decimal to float để tránh lỗi
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            open_price = float(df.iloc[0]['open'])
            close_price = float(df.iloc[-1]['close'])
            high_price = float(df['high'].max())
            low_price = float(df['low'].min())
            volume = float(df['volume'].sum())
            change_pct = ((close_price - open_price) / open_price) * 100

            formatted_text = f"""
### 📊 Dữ Liệu Chính Xác từ Database

**{ticker}** - Ngày {date_str}

| Chỉ Số | Giá Trị |
|--------|---------|
| Mở Cửa | ${open_price:,.2f} |
| Đóng Cửa | ${close_price:,.2f} |
| Cao Nhất | ${high_price:,.2f} |
| Thấp Nhất | ${low_price:,.2f} |
| Biến Động | {change_pct:+.2f}% |
| Volume | {volume:,.2f} |

{'📈 Tăng' if change_pct > 0 else '📉 Giảm'} **{abs(change_pct):.2f}%** so với giá mở cửa.

*Nguồn: Database (nến 5 phút)*
"""
            
            return {
                'status': 'success',
                'source': 'database',
                'data': {
                    'ticker': ticker,
                    'date': date_str,
                    'open': open_price,
                    'close': close_price,
                    'high': high_price,
                    'low': low_price,
                    'volume': volume,
                    'change_pct': change_pct,
                    'candles_count': len(df)
                },
                'formatted': formatted_text.strip()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'source': 'database',
                'message': f"Lỗi truy vấn database: {str(e)}",
                'error': str(e),
                'query_info': {
                    'ticker': ticker,
                    'date': date_str,
                    'type': 'exact_price'
                }
            }
    
    @staticmethod
    def get_price_range(ticker: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Lấy dữ liệu trong khoảng thời gian
        
        Args:
            ticker: Mã coin
            start_date: Ngày bắt đầu (YYYY-MM-DD)
            end_date: Ngày kết thúc (YYYY-MM-DD)
            
        Returns:
            Dict với status, source, data, formatted
        """
        try:
            start_ts = pd.to_datetime(start_date).replace(hour=0, minute=0)
            end_ts = pd.to_datetime(end_date).replace(hour=23, minute=59)
            
            df = ClickhouseHelper.get_data_between(
                ticker=ticker,
                time_start=start_ts,
                time_end=end_ts,
                interval=Interval.FIVE_MINUTES,
                verbose=False
            )
            
            if df.empty:
                return {
                    'status': 'no_data',
                    'source': 'database',
                    'message': f"Database không có dữ liệu cho {ticker} từ {start_date} đến {end_date}",
                    'suggestion': 'Thử chọn khoảng thời gian gần hơn hoặc ticker khác',
                    'query_info': {
                        'ticker': ticker,
                        'start_date': start_date,
                        'end_date': end_date,
                        'type': 'price_range'
                    }
                }
            
            # Convert Decimal to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            start_price = float(df.iloc[0]['open'])
            end_price = float(df.iloc[-1]['close'])
            high = float(df['high'].max())
            low = float(df['low'].min())
            total_volume = float(df['volume'].sum())
            avg_volume = float(df['volume'].mean())
            change_pct = ((end_price - start_price) / start_price) * 100
            
            # Tính volatility (std deviation của close prices)
            volatility = float(df['close'].std())
            volatility_pct = (volatility / df['close'].mean()) * 100
            
            # Số ngày trading
            days_count = (end_ts - start_ts).days + 1
            
            formatted_text = f"""
### 📊 Dữ Liệu Khoảng Thời Gian

**{ticker}** - {start_date} → {end_date} ({days_count} ngày)

| Chỉ Số | Giá Trị |
|--------|---------|
| Giá Đầu Kỳ | ${start_price:,.2f} |
| Giá Cuối Kỳ | ${end_price:,.2f} |
| Đỉnh | ${high:,.2f} (+{((high - start_price) / start_price * 100):,.2f}%) |
| Đáy | ${low:,.2f} ({((low - start_price) / start_price * 100):,.2f}%) |
| Biến Động | {change_pct:+.2f}% |
| Volatility | {volatility_pct:.2f}% |
| Volume TB/Ngày | {avg_volume:,.2f} |
| Tổng Volume | {total_volume:,.2f} |

{'📈 Tăng' if change_pct > 0 else '📉 Giảm'} **{abs(change_pct):.2f}%** trong kỳ.
Biên độ dao động: **{((high - low) / low * 100):.2f}%**

*Nguồn: Database ({len(df):,} nến 5 phút)*
"""
            
            return {
                'status': 'success',
                'source': 'database',
                'data': {
                    'ticker': ticker,
                    'start_date': start_date,
                    'end_date': end_date,
                    'days_count': days_count,
                    'start_price': start_price,
                    'end_price': end_price,
                    'high': high,
                    'low': low,
                    'change_pct': change_pct,
                    'volatility': volatility,
                    'volatility_pct': volatility_pct,
                    'total_volume': total_volume,
                    'avg_volume': avg_volume,
                    'candles_count': len(df)
                },
                'formatted': formatted_text.strip()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'source': 'database',
                'message': f"Lỗi truy vấn database: {str(e)}",
                'error': str(e),
                'query_info': {
                    'ticker': ticker,
                    'start_date': start_date,
                    'end_date': end_date,
                    'type': 'price_range'
                }
            }
        
    @staticmethod
    def get_price_at_moment(ticker: str, date_time_str: str) -> Dict[str, Any]:
        """
        Lấy giá tại một thời điểm cụ thể (YYYY-MM-DD HH:MM)
        Query nến 5m gần nhất trước thời điểm đó
        
        Args:
            ticker: Mã coin
            date_time_str: Thời điểm (YYYY-MM-DD HH:MM)
            
        Returns:
            Dict với status, source, data, formatted
        """
        try:
            target_dt = pd.to_datetime(date_time_str)
            target_ts = int(target_dt.timestamp() * 1000)  # Convert to ms for ClickHouse

            # Query: Lấy 1 bản ghi có thời gian <= target_time (Nến gần nhất)
            query = f"""
            SELECT openTime, open, high, low, close, volume 
            FROM future_kline_5m 
            WHERE ticker = '{ticker}' AND openTime <= {target_ts} 
            ORDER BY openTime DESC 
            LIMIT 1
            """
            
            df = ClickhouseHelper.run_to_df(query, verbose=False)

            if df.empty:
                return {
                    'status': 'no_data',
                    'source': 'database',
                    'message': f"Database không có dữ liệu cho {ticker} tại thời điểm {date_time_str}",
                    'suggestion': 'Thử chọn thời điểm gần hơn hoặc ticker khác',
                    'query_info': {
                        'ticker': ticker,
                        'datetime': date_time_str,
                        'type': 'price_at_moment'
                    }
                }

            row = df.iloc[0]
            
            # Convert to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                row[col] = float(pd.to_numeric(row[col], errors='coerce'))
            
            candle_time = pd.to_datetime(row['openTime'], unit='ms')
            time_diff = abs((target_dt - candle_time).total_seconds() / 60)  # minutes
            
            formatted_text = f"""
### 📊 Giá Tại Thời Điểm Cụ Thể

**{ticker}** - {date_time_str}

**Nến 5 phút gần nhất:** {candle_time.strftime('%Y-%m-%d %H:%M')}  
*(Cách {int(time_diff)} phút từ thời điểm yêu cầu)*

| Chỉ Số | Giá Trị |
|--------|---------|
| Giá Mở | ${row['open']:,.2f} |
| Giá Đóng | ${row['close']:,.2f} |
| Cao Nhất | ${row['high']:,.2f} |
| Thấp Nhất | ${row['low']:,.2f} |
| Volume | {row['volume']:,.2f} |

{'📈 Nến Tăng' if row['close'] >= row['open'] else '📉 Nến Giảm'} - Biên độ: **{((row['high'] - row['low']) / row['low'] * 100):.2f}%**

*Nguồn: Database (nến 5 phút gần nhất)*
"""
            
            return {
                'status': 'success',
                'source': 'database',
                'data': {
                    'ticker': ticker,
                    'requested_time': date_time_str,
                    'actual_candle_time': candle_time.strftime('%Y-%m-%d %H:%M'),
                    'time_diff_minutes': int(time_diff),
                    'open': row['open'],
                    'close': row['close'],
                    'high': row['high'],
                    'low': row['low'],
                    'volume': row['volume']
                },
                'formatted': formatted_text.strip()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'source': 'database',
                'message': f"Lỗi truy vấn thời điểm: {str(e)}",
                'error': str(e),
                'query_info': {
                    'ticker': ticker,
                    'datetime': date_time_str,
                    'type': 'price_at_moment'
                }
            }

    @staticmethod
    def get_market_overview(ticker: str) -> Dict[str, Any]:
        """
        Lấy dữ liệu thị trường mới nhất (24h gần nhất)
        
        Args:
            ticker: Mã coin
            
        Returns:
            Dict với status, source, data, formatted
        """
        try:
            df = ClickhouseHelper.get_latest_data(ticker=ticker, limit=288, verbose=False)  # 288 nến 5m = 24h
            
            if df.empty:
                return {
                    'status': 'no_data',
                    'source': 'database',
                    'message': f"Database không có dữ liệu realtime cho {ticker}",
                    'suggestion': 'Thử ticker khác hoặc kiểm tra kết nối database',
                    'query_info': {
                        'ticker': ticker,
                        'type': 'market_overview'
                    }
                }
            
            # Convert to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            latest = df.iloc[-1]
            prev_24h = df.iloc[0]
            
            current_price = float(latest['close'])
            prev_price = float(prev_24h['close'])
            high_24h = float(df['high'].max())
            low_24h = float(df['low'].min())
            volume_24h = float(df['volume'].sum())
            
            change_24h = ((current_price - prev_price) / prev_price) * 100
            
            # Tính thêm các metrics
            high_change = ((high_24h - prev_price) / prev_price) * 100
            low_change = ((low_24h - prev_price) / prev_price) * 100
            
            # Last update time
            if 'openTime' in latest:
                last_update = pd.to_datetime(latest['openTime'], unit='ms')
            else:
                last_update = datetime.now()
            
            formatted_text = f"""
### 📊 Thị Trường Hiện Tại (24h)

**{ticker}** - Cập nhật: {last_update.strftime('%H:%M %d/%m/%Y')}

| Chỉ Số | Giá Trị |
|--------|---------|
| Giá Hiện Tại | ${current_price:,.2f} |
| Thay Đổi 24h | {change_24h:+.2f}% |
| Cao 24h | ${high_24h:,.2f} ({high_change:+.2f}%) |
| Thấp 24h | ${low_24h:,.2f} ({low_change:+.2f}%) |
| Volume 24h | {volume_24h:,.2f} |

{'📈 Xu Hướng Tăng' if change_24h > 0 else '📉 Xu Hướng Giảm'} - Biên độ 24h: **{((high_24h - low_24h) / low_24h * 100):.2f}%**

*Nguồn: Database (dữ liệu realtime)*
"""
            
            return {
                'status': 'success',
                'source': 'database',
                'data': {
                    'ticker': ticker,
                    'current_price': current_price,
                    'change_24h': change_24h,
                    'high_24h': high_24h,
                    'low_24h': low_24h,
                    'volume_24h': volume_24h,
                    'last_update': last_update.strftime('%Y-%m-%d %H:%M'),
                    'candles_count': len(df)
                },
                'formatted': formatted_text.strip()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'source': 'database',
                'message': f"Lỗi truy vấn market overview: {str(e)}",
                'error': str(e),
                'query_info': {
                    'ticker': ticker,
                    'type': 'market_overview'
                }
            }
    
    @staticmethod
    def format_error_response(error_dict: Dict[str, Any]) -> str:
        """
        Format error dict thành message cho LLM
        
        Args:
            error_dict: Dict từ các hàm get_* khi status != 'success'
            
        Returns:
            Formatted error message
        """
        if error_dict['status'] == 'no_data':
            return f"""
⚠️ **KHÔNG CÓ DỮ LIỆU TRONG DATABASE**

{error_dict['message']}

💡 **Gợi ý:** {error_dict.get('suggestion', 'Thử với tham số khác')}

*Query Info: {error_dict.get('query_info', {})}*
"""
        elif error_dict['status'] == 'error':
            return f"""
❌ **LỖI TRUY VẤN**

{error_dict['message']}

*Technical Details: {error_dict.get('error', 'N/A')}*
"""
        else:
            return f"⚠️ Unknown error: {error_dict}"
    
    @staticmethod
    def extract_data_summary(result_dict: Dict[str, Any]) -> str:
        """
        Trích xuất summary ngắn gọn từ result dict (cho logging/debugging)
        
        Args:
            result_dict: Dict từ các hàm get_*
            
        Returns:
            Short summary string
        """
        if result_dict['status'] != 'success':
            return f"[{result_dict['status'].upper()}] {result_dict.get('message', 'N/A')}"
        
        data = result_dict.get('data', {})
        ticker = data.get('ticker', 'N/A')
        
        if 'current_price' in data:
            # Market overview
            return f"[SUCCESS] {ticker}: ${data['current_price']:,.2f} ({data['change_24h']:+.2f}% 24h)"
        elif 'close' in data and 'open' not in data:
            # Price at moment
            return f"[SUCCESS] {ticker} @ {data.get('actual_candle_time', 'N/A')}: ${data['close']:,.2f}"
        elif 'change_pct' in data and 'start_price' in data:
            # Price range
            return f"[SUCCESS] {ticker} {data.get('start_date', '')}→{data.get('end_date', '')}: {data['change_pct']:+.2f}%"
        elif 'change_pct' in data:
            # Exact price
            return f"[SUCCESS] {ticker} {data.get('date', '')}: {data['change_pct']:+.2f}%"
        else:
            return f"[SUCCESS] {ticker} - Data retrieved"