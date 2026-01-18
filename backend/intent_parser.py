from datetime import datetime, timedelta
import re
from typing import Dict, Any

class IntentParser:
    RELATIVE_TIME_MAP = {
        'hôm nay': 0,
        'today': 0,
        'hôm qua': -1,
        'yesterday': -1,
        'hôm kia': -2,
        'tuần này': 'this_week',
        'tuần trước': 'last_week',
        'tháng này': 'this_month',
        'tháng trước': 'last_month',
        'năm nay': 'this_year',
        'năm ngoái': 'last_year'
    }
    
    @staticmethod
    def parse_intent(user_message: str) -> Dict[str, Any]:
        """Parse ý định người dùng NHANH mà không cần LLM"""
        message_lower = user_message.lower()
        
        intent = {
            'type': 'general',  # price_query, trend_analysis, news_summary, general
            'ticker': 'BTCUSDT',
            'time_range': None,
            'exact_time': None,
            'requires_structured_data': False,
            'requires_vector_search': True
        }
        
        # 1. Extract Ticker
        ticker_match = re.search(r'\b(BTC|ETH|BNB|SOL|ADA|DOT|MATIC|LINK)(?:USDT)?\b', 
                                message_lower, re.IGNORECASE)
        if ticker_match:
            ticker = ticker_match.group(1).upper()
            intent['ticker'] = f"{ticker}USDT" if not ticker.endswith('USDT') else ticker
        
        # 2. Detect Query Type
        if any(kw in message_lower for kw in ['giá', 'price', 'bao nhiêu', 'how much']):
            intent['type'] = 'price_query'
            intent['requires_structured_data'] = True
            
        elif any(kw in message_lower for kw in ['xu hướng', 'trend', 'phân tích', 'analyze']):
            intent['type'] = 'trend_analysis'
            intent['requires_structured_data'] = True
            intent['requires_vector_search'] = True
            
        elif any(kw in message_lower for kw in ['tin tức', 'news', 'sentiment', 'tâm lý']):
            intent['type'] = 'news_summary'
            intent['requires_vector_search'] = True
        
        # 3. Parse Time (QUAN TRỌNG!)
        now = datetime.now()
        
        # Exact date format: 2024-01-15, 15/01/2024, 15-01-2024
        date_patterns = [
            r'(\d{4})-(\d{2})-(\d{2})',  # YYYY-MM-DD
            r'(\d{2})/(\d{2})/(\d{4})',  # DD/MM/YYYY
            r'(\d{2})-(\d{2})-(\d{4})'   # DD-MM-YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, user_message)
            if match:
                try:
                    if '-' in pattern and pattern.startswith(r'(\d{4})'):
                        date_obj = datetime.strptime(match.group(0), '%Y-%m-%d')
                    else:
                        date_obj = datetime.strptime(match.group(0), '%d/%m/%Y')
                    intent['exact_time'] = date_obj.strftime('%Y-%m-%d')
                    return intent
                except:
                    pass
        
        # Relative time
        for keyword, offset in IntentParser.RELATIVE_TIME_MAP.items():
            if keyword in message_lower:
                if isinstance(offset, int):
                    # Days offset
                    target_date = now + timedelta(days=offset)
                    intent['exact_time'] = target_date.strftime('%Y-%m-%d')
                elif offset == 'this_week':
                    intent['time_range'] = {
                        'start': (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d'),
                        'end': now.strftime('%Y-%m-%d')
                    }
                elif offset == 'last_week':
                    start = now - timedelta(days=now.weekday() + 7)
                    end = start + timedelta(days=6)
                    intent['time_range'] = {
                        'start': start.strftime('%Y-%m-%d'),
                        'end': end.strftime('%Y-%m-%d')
                    }
                elif offset == 'this_month':
                    intent['time_range'] = {
                        'start': now.replace(day=1).strftime('%Y-%m-%d'),
                        'end': now.strftime('%Y-%m-%d')
                    }
                elif offset == 'last_month':
                    last_month = now.replace(day=1) - timedelta(days=1)
                    intent['time_range'] = {
                        'start': last_month.replace(day=1).strftime('%Y-%m-%d'),
                        'end': last_month.strftime('%Y-%m-%d')
                    }
                return intent
            
        # Detect "X ngày gần nhất/qua"
        days_pattern = r'(\d+)\s*ngày\s*(gần nhất|qua|trước|gần đây)'
        match = re.search(days_pattern, message_lower)
        if match:
            days_count = int(match.group(1))
            end_date = now
            start_date = now - timedelta(days=days_count)
            intent['time_range'] = {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
            intent['requires_structured_data'] = True
        
        # Default: recent data (7 days)
        if intent['type'] == 'trend_analysis':
            intent['time_range'] = {
                'start': (now - timedelta(days=7)).strftime('%Y-%m-%d'),
                'end': now.strftime('%Y-%m-%d')
            }
        
        # 3d. Giờ/phút trước
        hour_pattern = r'(\d+)\s*(giờ|hour|h)\s*(trước|ago|qua)'
        match = re.search(hour_pattern, message_lower)
        if match:
            hours = int(match.group(1))
            target = now - timedelta(hours=hours)
            intent['exact_time'] = target.strftime('%Y-%m-%d %H:%M')
            intent['requires_structured_data'] = True
            return intent

        minute_pattern = r'(\d+)\s*(phút|minute|min|m)\s*(trước|ago|qua)'
        match = re.search(minute_pattern, message_lower)
        if match:
            minutes = int(match.group(1))
            target = now - timedelta(minutes=minutes)
            intent['exact_time'] = target.strftime('%Y-%m-%d %H:%M')
            intent['requires_structured_data'] = True
            return intent

        # 3e. Thời gian tương lai
        if 'ngày mai' in message_lower or 'tomorrow' in message_lower:
            intent['exact_time'] = (now + timedelta(days=1)).strftime('%Y-%m-%d')
            return intent

        if 'tuần sau' in message_lower or 'next week' in message_lower:
            start = now + timedelta(days=(7 - now.weekday()))
            end = start + timedelta(days=6)
            intent['time_range'] = {
                'start': start.strftime('%Y-%m-%d'),
                'end': end.strftime('%Y-%m-%d')
            }
            return intent

        # 3f. Thời điểm cụ thể: "lúc 10h ngày 15/1" hoặc "15/1 lúc 10h"
        time_specific_pattern = r'(?:lúc\s*)?(\d{1,2})[:h]\s*(\d{0,2}).*?(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?|(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\s*(?:lúc\s*)?(\d{1,2})[:h]\s*(\d{0,2})'
        match = re.search(time_specific_pattern, message_lower)
        if match:
            groups = match.groups()
            try:
                if groups[0]:  # "lúc 10h ngày 15/1"
                    hour = int(groups[0])
                    minute = int(groups[1]) if groups[1] else 0
                    day = int(groups[2])
                    month = int(groups[3])
                    year = int(groups[4]) if groups[4] else now.year
                else:  # "15/1 lúc 10h"
                    day = int(groups[5])
                    month = int(groups[6])
                    year = int(groups[7]) if groups[7] else now.year
                    hour = int(groups[8])
                    minute = int(groups[9]) if groups[9] else 0
                
                if year < 100:
                    year += 2000
                
                target = datetime(year, month, day, hour, minute)
                intent['exact_time'] = target.strftime('%Y-%m-%d %H:%M')
                intent['requires_structured_data'] = True
                return intent
            except:
                pass

        return intent

intent_parser = IntentParser()